# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Compare plans of the schedulable-TI query ordered by run_after (PR) vs logical_date (main).

Throwaway. Run inside breeze against the Postgres or MySQL backend:

    breeze run --backend postgres bash -c \
        "airflow db migrate && python /opt/airflow/dev/bench_schedulable_tis_order.py --seed"
    breeze run --backend mysql bash -c \
        "airflow db migrate && python /opt/airflow/dev/bench_schedulable_tis_order.py --seed"

The query comes from the checked-out ``_build_schedulable_tis_query`` (expected to order by
``run_after``); the ``logical_date`` variant is derived by swapping the ordering column back at
the three sites the PR changed.

MySQL has no EXPLAIN with buffer counts or a JSON plan carrying actual times, so there the
variants are timed by executing the query itself, and sort and temp-table work is read from the
session status counters around each execution.
"""

from __future__ import annotations

import argparse
import difflib
import json
import random
import re
import statistics
import time
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, insert

from airflow import settings
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.dag import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.sqlalchemy import with_row_locks
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

DAG_PREFIX = "bench_order_"
BUNDLE = "bench_order_bundle"
BASE = datetime(2026, 1, 1, tzinfo=UTC)
PRIORITY_WEIGHTS = [1, 1, 1, 1, 2, 3, 5, 10]
POOLS = ["default_pool", "default_pool", "default_pool", "pool_a", "pool_b"]
ACTIVE_STATES = [TaskInstanceState.RUNNING] * 4 + [TaskInstanceState.QUEUED] * 2
FLUSH_EVERY = 20_000

NEW_COL = re.compile(r"\bdag_run\.run_after\b")
NEW_LABEL = re.compile(r"\brun_after_for_ordering\b")

MYSQL_STATUS_VARS = (
    "Sort_merge_passes",
    "Sort_rows",
    "Sort_scan",
    "Sort_range",
    "Created_tmp_tables",
    "Created_tmp_disk_tables",
    "Handler_read_key",
    "Handler_read_next",
    "Handler_read_rnd_next",
)
MYSQL_TREE_NUMBERS = re.compile(r"\s*\((?:cost|actual time)=[^)]*\)")
MYSQL_ORDER_COL = re.compile(r"\b(?:run_after|logical_date)(?:_for_ordering)?\b")
MYSQL_ROOT_ACTUAL = re.compile(r"actual time=[\d.]+\.\.([\d.]+)")


def build_run_rows(dag_id: str, d: int, history_runs: int, running_runs: int):
    """Yield (row, is_running) for one Dag; running runs mix scheduled, manual and asset-triggered."""
    dag_offset = timedelta(minutes=(d % 12) * 5)
    for h in range(history_runs):
        history_date = BASE + timedelta(hours=h) + dag_offset
        yield (
            {
                "dag_id": dag_id,
                "run_id": f"scheduled__{h}",
                "run_type": DagRunType.SCHEDULED.value,
                "state": DagRunState.SUCCESS.value,
                "logical_date": history_date,
                "data_interval_start": history_date,
                "data_interval_end": history_date + timedelta(hours=1),
                "run_after": history_date + timedelta(hours=1),
                "log_template_id": None,
            },
            False,
        )
    for r in range(running_runs):
        k = history_runs + r
        slot = BASE + timedelta(hours=k) + dag_offset
        jitter = timedelta(minutes=random.randint(0, 59))
        kind = r % 10
        logical_date: datetime | None
        interval: tuple[datetime | None, datetime | None]
        if kind <= 6:
            run_type, logical_date, run_after = DagRunType.SCHEDULED, slot, slot + timedelta(hours=1)
            interval = (slot, slot + timedelta(hours=1))
        elif kind == 7:
            run_type, logical_date, run_after = DagRunType.MANUAL, slot + jitter, slot + jitter
            interval = (slot + jitter, slot + jitter)
        else:
            run_type, logical_date, run_after = DagRunType.ASSET_TRIGGERED, None, slot + jitter
            interval = (None, None)
        yield (
            {
                "dag_id": dag_id,
                "run_id": f"{run_type.value}__{k}",
                "run_type": run_type.value,
                "state": DagRunState.RUNNING.value,
                "logical_date": logical_date,
                "data_interval_start": interval[0],
                "data_interval_end": interval[1],
                "run_after": run_after,
                "log_template_id": None,
            },
            True,
        )


def build_ti_rows(dag_id: str, run_id: str, is_running: bool, tis_per_run: int):
    n_mapped = min(10, tis_per_run // 4)
    keys = [(f"task_{i}", -1) for i in range(tis_per_run - n_mapped)]
    keys += [("mapped", i) for i in range(n_mapped)]
    state = TaskInstanceState.SCHEDULED if is_running else TaskInstanceState.SUCCESS
    for task_id, region_index in keys:
        yield {
            "dag_id": dag_id,
            "run_id": run_id,
            "task_id": task_id,
            "region_index": region_index,
            "state": state.value,
            "pool": random.choice(POOLS),
            "priority_weight": random.choice(PRIORITY_WEIGHTS),
        }
    if is_running:
        for j, active_state in enumerate(ACTIVE_STATES):
            yield {
                "dag_id": dag_id,
                "run_id": run_id,
                "task_id": f"active_{j}",
                "region_index": -1,
                "state": active_state.value,
                "pool": "default_pool",
                "priority_weight": 1,
            }


def seed(args) -> None:
    random.seed(42)
    engine = settings.engine
    started = time.monotonic()
    with engine.begin() as conn:
        like = f"{DAG_PREFIX}%"
        conn.execute(delete(TaskInstance.__table__).where(TaskInstance.__table__.c.dag_id.like(like)))
        conn.execute(delete(DagRun.__table__).where(DagRun.__table__.c.dag_id.like(like)))
        conn.execute(delete(DagModel.__table__).where(DagModel.__table__.c.dag_id.like(like)))
        conn.execute(delete(DagBundleModel.__table__).where(DagBundleModel.__table__.c.name == BUNDLE))
        conn.execute(insert(DagBundleModel.__table__), [{"name": BUNDLE, "active": True}])

        dag_ids = [f"{DAG_PREFIX}{d:04d}" for d in range(args.dags)]
        conn.execute(
            insert(DagModel.__table__),
            [
                {
                    "dag_id": dag_id,
                    "bundle_name": BUNDLE,
                    "is_paused": False,
                    "is_stale": False,
                    "max_active_tasks": 64 if d % 5 == 0 else 16,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "has_task_concurrency_limits": False,
                }
                for d, dag_id in enumerate(dag_ids)
            ],
        )

        run_buf: list[dict] = []
        ti_buf: list[dict] = []
        n_runs = n_tis = 0
        for d, dag_id in enumerate(dag_ids):
            for run_row, is_running in build_run_rows(dag_id, d, args.history_runs, args.runs):
                run_buf.append(run_row)
                ti_buf.extend(build_ti_rows(dag_id, run_row["run_id"], is_running, args.tis))
            if len(ti_buf) >= FLUSH_EVERY or d == len(dag_ids) - 1:
                conn.execute(insert(DagRun.__table__), run_buf)
                conn.execute(insert(TaskInstance.__table__), ti_buf)
                n_runs += len(run_buf)
                n_tis += len(ti_buf)
                run_buf, ti_buf = [], []
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        if engine.dialect.name == "mysql":
            conn.exec_driver_sql("ANALYZE TABLE dag, dag_run, task_instance").fetchall()
        else:
            conn.exec_driver_sql("VACUUM ANALYZE dag, dag_run, task_instance")
    print(f"Seeded {args.dags} Dags, {n_runs} runs, {n_tis} TIs in {time.monotonic() - started:.1f}s")


def build_sql_pair(max_tis: int) -> tuple[str, str]:
    """Return (old, new) SQL: new as built by the checked-out code, old with logical_date swapped back."""
    runner = SchedulerJobRunner(job=Job())
    query = runner._build_schedulable_tis_query(
        starved_pools=set(),
        starved_dag_runs=set(),
        starved_tasks=set(),
        starved_tasks_task_dagrun_concurrency=set(),
        max_tis=max_tis,
    )
    session = settings.Session()
    try:
        locked = with_row_locks(query, of=TaskInstance, session=session, skip_locked=True)
        new_sql = str(
            locked.compile(dialect=session.get_bind().dialect, compile_kwargs={"literal_binds": True})
        )
    finally:
        session.close()

    # Three sites in the ranked subquery (inner ORDER BY, window ORDER BY, label) and the outer ORDER BY.
    col_hits, label_hits = len(NEW_COL.findall(new_sql)), len(NEW_LABEL.findall(new_sql))
    if (col_hits, label_hits) != (3, 2) or "logical_date_for_ordering" in new_sql:
        raise SystemExit(
            f"Unexpected query shape: {col_hits} x dag_run.run_after, {label_hits} x run_after_for_ordering."
            " Is the PR commit checked out?"
        )
    old_sql = NEW_LABEL.sub("logical_date_for_ordering", NEW_COL.sub("dag_run.logical_date", new_sql))
    return old_sql, new_sql


def explain_json(sql: str) -> dict:
    with settings.engine.connect() as conn:
        raw = conn.exec_driver_sql(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}").scalar()
        conn.rollback()
    return (json.loads(raw) if isinstance(raw, str) else raw)[0]


def explain_text(sql: str) -> str:
    with settings.engine.connect() as conn:
        rows = conn.exec_driver_sql(f"EXPLAIN (ANALYZE, BUFFERS) {sql}").scalars().all()
        conn.rollback()
    return "\n".join(rows)


def iter_nodes(node: dict):
    yield node
    for child in node.get("Plans", []):
        yield from iter_nodes(child)


def compute_shape(node: dict) -> tuple:
    """Plan structure without runtime numbers or sort-key names, which differ by design."""
    return (
        node["Node Type"],
        node.get("Parent Relationship"),
        node.get("Relation Name"),
        node.get("Alias"),
        node.get("Index Name"),
        node.get("Join Type"),
        node.get("Strategy"),
        tuple(compute_shape(child) for child in node.get("Plans", [])),
    )


def extract_sorts(plan: dict) -> list[str]:
    out = []
    for node in iter_nodes(plan):
        if "Sort Key" not in node:
            continue
        detail = node.get("Sort Method") or ""
        if "Sort Space Used" in node:
            detail += f" {node['Sort Space Used']}kB {node.get('Sort Space Type', '')}"
        if "Presorted Key" in node:
            detail += f" presorted={node['Presorted Key']}"
        for group in ("Full-sort Groups", "Pre-sorted Groups"):
            if group in node:
                g = node[group]
                detail += f" {group}: {g.get('Sort Methods Used')} peak={g.get('Peak Sort Space Used')}kB"
        out.append(f"{node['Node Type']} [{', '.join(node['Sort Key'])}] {detail.strip()}")
    return out


def summarize(results: list[dict]) -> dict:
    exec_ms = [r["Execution Time"] for r in results]
    plan_ms = [r["Planning Time"] for r in results]
    top = results[-1]["Plan"]
    return {
        "exec": (statistics.median(exec_ms), min(exec_ms), max(exec_ms)),
        "plan": statistics.median(plan_ms),
        "jit": statistics.median(r.get("JIT", {}).get("Timing", {}).get("Total", 0.0) for r in results),
        "cost": top["Total Cost"],
        "rows": top["Actual Rows"],
        "hit": top.get("Shared Hit Blocks"),
        "read": top.get("Shared Read Blocks"),
        "temp": (top.get("Temp Read Blocks"), top.get("Temp Written Blocks")),
        "shapes": {compute_shape(r["Plan"]) for r in results},
        "indexes": sorted({n["Index Name"] for n in iter_nodes(top) if "Index Name" in n}),
        "sorts": extract_sorts(top),
    }


def read_mysql_session_status(conn) -> dict[str, int]:
    names = ", ".join(f"'{name}'" for name in MYSQL_STATUS_VARS)
    rows = conn.exec_driver_sql(f"SHOW SESSION STATUS WHERE Variable_name IN ({names})").fetchall()
    return {name: int(value) for name, value in rows}


def time_mysql_query(sql: str) -> dict:
    """Execute and fetch once; status deltas are net of what SHOW STATUS itself adds to the counters."""
    with settings.engine.connect() as conn:
        first = read_mysql_session_status(conn)
        second = read_mysql_session_status(conn)
        started = time.monotonic()
        rows = conn.exec_driver_sql(sql).fetchall()
        elapsed_ms = (time.monotonic() - started) * 1000
        third = read_mysql_session_status(conn)
        conn.rollback()
    return {
        "ms": elapsed_ms,
        "rows": len(rows),
        "status": {k: (third[k] - second[k]) - (second[k] - first[k]) for k in MYSQL_STATUS_VARS},
    }


def explain_mysql(sql: str, *, analyze: bool) -> str:
    with settings.engine.connect() as conn:
        tree = conn.exec_driver_sql(f"EXPLAIN {'ANALYZE' if analyze else 'FORMAT=TREE'} {sql}").scalar()
        conn.rollback()
    return tree


def normalize_mysql_tree(tree: str) -> str:
    """Plan structure without cost or timing numbers, and with the ordering column masked."""
    return MYSQL_ORDER_COL.sub("<order_col>", MYSQL_TREE_NUMBERS.sub("", tree))


def bench_mysql(args, max_tis: int, variants: dict[str, str]) -> None:
    for name, sql in variants.items():
        print(f"\n{'=' * 20} max_tis={max_tis} {name} EXPLAIN ANALYZE, warm-up run {'=' * 20}")
        print(explain_mysql(sql, analyze=True))

    shapes = {name: normalize_mysql_tree(explain_mysql(sql, analyze=False)) for name, sql in variants.items()}
    results: dict[str, list[dict]] = {"old": [], "new": []}
    for rep in range(args.reps):
        for name in ("old", "new") if rep % 2 == 0 else ("new", "old"):
            results[name].append(time_mysql_query(variants[name]))
    analyzed = {name: explain_mysql(sql, analyze=True) for name, sql in variants.items()}

    print(f"\n{'#' * 20} max_tis={max_tis}, {args.reps} reps, alternating order {'#' * 20}")
    print(
        f"{'variant':8} {'exec ms med/min/max':>24} {'rows':>6} {'analyze root ms':>16}  status deltas (median)"
    )
    for name, res in results.items():
        ms = [r["ms"] for r in res]
        root = MYSQL_ROOT_ACTUAL.search(analyzed[name])
        status = {k: statistics.median(r["status"][k] for r in res) for k in MYSQL_STATUS_VARS}
        print(
            f"{name:8} {statistics.median(ms):>8.1f}/{min(ms):>7.1f}/{max(ms):>7.1f} {res[-1]['rows']:>6}"
            f" {root.group(1) if root else '?':>16}  " + ", ".join(f"{k}={v:g}" for k, v in status.items())
        )
    same = shapes["old"] == shapes["new"]
    print(f"plan shape identical across variants: {same}")
    if not same:
        print(
            "\n".join(
                difflib.unified_diff(shapes["old"].splitlines(), shapes["new"].splitlines(), lineterm="")
            )
        )
    for name, tree in analyzed.items():
        print(f"  {name} sorts:")
        for line in tree.splitlines():
            if "Sort" in line:
                print(f"    {line.strip()}")


def bench_postgres(args, max_tis: int, variants: dict[str, str]) -> None:
    for name, sql in variants.items():
        print(f"\n{'=' * 20} max_tis={max_tis} {name} EXPLAIN (ANALYZE, BUFFERS), warm-up run {'=' * 20}")
        print(explain_text(sql))

    results: dict[str, list[dict]] = {"old": [], "new": []}
    for rep in range(args.reps):
        for name in ("old", "new") if rep % 2 == 0 else ("new", "old"):
            results[name].append(explain_json(variants[name]))

    summaries = {name: summarize(res) for name, res in results.items()}
    print(f"\n{'#' * 20} max_tis={max_tis}, {args.reps} reps, alternating order {'#' * 20}")
    print(
        f"{'variant':8} {'exec ms med/min/max':>24} {'plan ms':>8} {'cost':>12} {'rows':>6}"
        f" {'jit ms':>7} {'shared hit':>11} {'read':>6} {'temp r/w':>10}"
    )
    for name, s in summaries.items():
        med, lo, hi = s["exec"]
        print(
            f"{name:8} {med:>8.1f}/{lo:>7.1f}/{hi:>7.1f} {s['plan']:>8.2f} {s['cost']:>12.1f} {s['rows']:>6}"
            f" {s['jit']:>7.1f} {s['hit']!s:>11} {s['read']!s:>6} {s['temp'][0]!s:>4}/{s['temp'][1]!s:<5}"
        )
    same = summaries["old"]["shapes"] == summaries["new"]["shapes"] and len(summaries["old"]["shapes"]) == 1
    print(f"plan shape identical across variants and reps: {same}")
    for name, s in summaries.items():
        print(f"  {name} distinct shapes: {len(s['shapes'])}; indexes: {s['indexes']}")
        for sort in s["sorts"]:
            print(f"    {sort}")


def bench(args) -> None:
    run_variant_bench = bench_mysql if settings.engine.dialect.name == "mysql" else bench_postgres
    for i, max_tis in enumerate(args.max_tis):
        old_sql, new_sql = build_sql_pair(max_tis)
        if i == 0:
            print("=" * 30, "NEW SQL (run_after)", "=" * 30)
            print(new_sql)
            print("=" * 30, "OLD SQL (logical_date)", "=" * 30)
            print(old_sql)
            print("=" * 30, "DIFF old -> new", "=" * 30)
            print(
                "\n".join(difflib.unified_diff(old_sql.splitlines(), new_sql.splitlines(), lineterm="", n=0))
            )
        run_variant_bench(args, max_tis, {"old": old_sql, "new": new_sql})


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--seed", action="store_true", help="(Re)create the bench Dags, runs and TIs first.")
    parser.add_argument("--dags", type=int, default=500)
    parser.add_argument("--runs", type=int, default=10, help="Running Dag runs per Dag.")
    parser.add_argument("--tis", type=int, default=40, help="Scheduled TIs per running run.")
    parser.add_argument(
        "--history-runs", type=int, default=20, help="Finished runs per Dag, with success TIs."
    )
    parser.add_argument("--reps", type=int, default=7)
    parser.add_argument("--max-tis", type=int, nargs="+", default=[16, 512])
    args = parser.parse_args()

    if settings.engine.dialect.name not in ("postgresql", "mysql"):
        raise SystemExit(f"Postgres or MySQL only, got {settings.engine.dialect.name}")
    if args.seed:
        seed(args)
    bench(args)


if __name__ == "__main__":
    main()
