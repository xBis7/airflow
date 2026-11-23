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
Performance integration test for the scheduler.
Creates a backlog of Dag runs, starts the Airflow processes and measures how far they get through
it within a fixed window, once per test case of the code under test. See README.md.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from typing import TYPE_CHECKING, Any

import pytest
import redis
from sqlalchemy import delete, select, tuple_

from airflow.configuration import conf
from airflow.models import DagRun
from airflow.utils.session import create_session
from airflow.utils.state import State

from tests_common.test_utils.integration_setup import (
    OTEL_COLLECTOR_HOST,
    OTEL_COLLECTOR_PORT,
    ProgressReporter,
    compute_usable_cpu_count,
    create_queued_dag_runs,
    ensure_postgres_max_connections,
    export_setting,
    print_to_terminal,
    serialize_and_get_dags,
    start_process,
    start_processes,
    terminate_processes,
    wait_for_dag_runs,
    wait_for_otel_collector,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from airflow.serialization.definitions.dag import SerializedDAG

log = logging.getLogger("integration.perf.test_perf")

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
DAG_FOLDER = os.path.join(TEST_DIR, "dags")
POLL_USAGE_SCRIPT = os.path.join(TEST_DIR, "poll_usage.py")

# "true" or "false" per test case, read by the code under test.
NEW_CODE_ENV_VAR = "PERF_TEST__NEW_CODE"

PROMETHEUS_URL = "http://breeze-prometheus:9090"
# Prometheus scrapes every 30s, and the collector keeps serving a stopped process's last values for
# minutes, so one scrape after the stop carries the final totals.
FINAL_SCRAPE_WAIT_SECONDS = 40
# Cumulative from process start, so the last value of each process is its total for the test case.
PROMETHEUS_TOTALS = {
    "loops": "histogram_count(airflow_scheduler_scheduler_loop_duration_milliseconds)",
    "loop_ms": "histogram_sum(airflow_scheduler_scheduler_loop_duration_milliseconds)",
    "dependency_checks": "histogram_count(airflow_dagrun_dependency_check_milliseconds)",
    "dependency_check_ms": "histogram_sum(airflow_dagrun_dependency_check_milliseconds)",
    "count_lookup_ms": "histogram_sum(airflow_perf_mapped_ti_count_duration_milliseconds)",
    "count_db_lookups": "airflow_perf_mapped_ti_count_db_lookups_total",
    "count_memo_hits": "airflow_perf_mapped_ti_count_memo_hits_total",
    "sql_statements": "airflow_perf_scheduler_db_queries_total",
    "sql_ms": "histogram_sum(airflow_perf_scheduler_db_duration_milliseconds)",
    "queued": "histogram_count(airflow_task_scheduled_duration_milliseconds)",
}

# Per Airflow process; Airflow's defaults times the API server workers exceed Postgres's limit.
DB_POOL_SIZE = 3
DB_MAX_OVERFLOW = 5
# Per Celery child, which runs one task at a time.
RESULT_BACKEND_POOL_SIZE = 1
RESULT_BACKEND_MAX_OVERFLOW = 1
# psql, the test process and the usage poller.
DB_CONNECTION_HEADROOM = 20

CPUS = compute_usable_cpu_count()

# The first three are process counts; the rest are Airflow settings, None keeps Airflow's default.
PROFILES: dict[str, dict[str, Any]] = {
    # Sized so the scheduler is the only constraint: every other cap is far above what the workers
    # can execute.
    "dynamic": {
        # More slots per worker rather than more workers: each worker process costs connections.
        "worker_count": 3,
        "worker_concurrency": max(2, CPUS * 2 // 3),
        # One worker saturates well before the scheduler does.
        "api_server_workers": max(4, CPUS // 3),
        "parallelism": 4096,
        "default_pool_task_slot_count": 4096,
        "max_active_tasks_per_dag": 4096,
        # Many runs of one Dag; the default of 16 would throttle them.
        "max_active_runs_per_dag": 4096,
        "max_tis_per_query": None,
    },
    # Fill in as needed; the process counts start at the tools' defaults.
    "custom": {
        "worker_count": 1,
        "worker_concurrency": CPUS,
        "api_server_workers": conf.getint("api", "workers"),
        "parallelism": None,
        "default_pool_task_slot_count": None,
        "max_active_tasks_per_dag": None,
        "max_active_runs_per_dag": None,
        "max_tis_per_query": None,
    },
}


def compute_required_db_connections(sizing: dict[str, Any]) -> int:
    """Peak connections the deployment can open against the metadata DB."""
    # Tasks use the Execution API and never connect.
    scheduler = 1
    airflow_processes = scheduler + sizing["api_server_workers"] + sizing["worker_count"]
    per_airflow_process = DB_POOL_SIZE + DB_MAX_OVERFLOW
    # One result-backend pool per Celery child.
    celery_children = sizing["worker_count"] * sizing["worker_concurrency"]
    per_celery_child = RESULT_BACKEND_POOL_SIZE + RESULT_BACKEND_MAX_OVERFLOW
    return (
        airflow_processes * per_airflow_process + celery_children * per_celery_child + DB_CONNECTION_HEADROOM
    )


def flush_celery_broker() -> None:
    """Drop every message in the broker, including those a stopped worker had prefetched."""
    # Celery's purge leaves the prefetched ones, which the broker hands out again later.
    redis.Redis.from_url(conf.get("celery", "broker_url")).flushdb()


def delete_unfinished_dag_runs(runs: list[tuple[str, str]]) -> int:
    """Delete the unfinished runs of ``runs`` with their task instances; return how many."""
    with create_session() as session:
        result = session.execute(
            delete(DagRun)
            .where(
                tuple_(DagRun.dag_id, DagRun.run_id).in_(runs),
                DagRun.state.not_in([State.SUCCESS, State.FAILED]),
            )
            .execution_options(synchronize_session=False)
        )
    return getattr(result, "rowcount", 0)


def compute_mean_dag_run_seconds(runs: list[tuple[str, str]]) -> float | None:
    """Mean time from running to success of the successful runs of ``runs``."""
    with create_session() as session:
        rows = session.execute(
            select(DagRun.start_date, DagRun.end_date).where(
                tuple_(DagRun.dag_id, DagRun.run_id).in_(runs), DagRun.state == State.SUCCESS
            )
        ).all()
    durations = [(end - start).total_seconds() for start, end in rows if start and end]
    return sum(durations) / len(durations) if durations else None


def query_prometheus_totals(arm: str, window_seconds: int) -> dict[str, float | None]:
    """Sum each of ``PROMETHEUS_TOTALS`` over the processes labelled ``arm``, by their last values."""
    totals: dict[str, float | None] = {}
    for name, expr in PROMETHEUS_TOTALS.items():
        # target_info carries the resource attributes, perf.arm among them, of each process.
        query = (
            f"sum(max_over_time(({expr} * on (exported_instance) group_left () "
            f'target_info{{perf_arm="{arm}"}})[{window_seconds}s:15s]))'
        )
        url = f"{PROMETHEUS_URL}/api/v1/query?{urllib.parse.urlencode({'query': query})}"
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                result = json.load(response)["data"]["result"]
        except (OSError, ValueError, KeyError) as error:
            log.warning("Prometheus query for %s failed: %r", name, error)
            result = []
        totals[name] = float(result[0]["value"][1]) if result else None
    return totals


def divide(numerator: float | None, denominator: float | None, scale: float = 1) -> float | None:
    if numerator is None or not denominator:
        return None
    return numerator / denominator / scale


SUMMARY_ROWS: list[tuple[str, Callable[[dict[str, Any]], float | None], str]] = [
    ("Dag runs succeeded", lambda r: r["succeeded"], "{:.0f}"),
    (
        "Time to finish every Dag run (s)",
        lambda r: r["elapsed"] if r["succeeded"] == r["runs"] else None,
        "{:.0f}",
    ),
    ("Dag run duration, mean (s)", lambda r: r["mean_dag_run_seconds"], "{:.0f}"),
    ("Task instances queued per minute", lambda r: divide(r["queued"], r["elapsed"], 1 / 60), "{:.1f}"),
    ("Scheduler loop, mean (s)", lambda r: divide(r["loop_ms"], r["loops"], 1000), "{:.2f}"),
    (
        "Dependency check per Dag run, mean (ms)",
        lambda r: divide(r["dependency_check_ms"], r["dependency_checks"]),
        "{:.0f}",
    ),
    (
        "Copy count lookups, time per loop (s)",
        lambda r: divide(r["count_lookup_ms"], r["loops"], 1000),
        "{:.2f}",
    ),
    (
        "Copy count lookups to the database per loop",
        lambda r: divide(r["count_db_lookups"], r["loops"]),
        "{:.0f}",
    ),
    (
        "Copy count lookups from the memo per loop",
        lambda r: divide(r["count_memo_hits"], r["loops"]),
        "{:.0f}",
    ),
    ("Scheduler SQL statements per loop", lambda r: divide(r["sql_statements"], r["loops"]), "{:.0f}"),
    ("Scheduler SQL time per loop (s)", lambda r: divide(r["sql_ms"], r["loops"], 1000), "{:.2f}"),
]


def format_summary(results: dict[str, dict[str, Any]]) -> str:
    """One row per measure, one column per test case."""
    label_width = max(len(label) for label, _, _ in SUMMARY_ROWS) + 2
    lines = [" " * label_width + "".join(f"{name:>16}" for name in results)]
    for label, compute, fmt in SUMMARY_ROWS:
        cells = []
        for result in results.values():
            value = compute(result)
            cells.append(f"{'n/a' if value is None else fmt.format(value):>16}")
        lines.append(f"{label:<{label_width}}" + "".join(cells))
    return "\n".join(lines)


@pytest.mark.integration("otel")
@pytest.mark.integration("redis")
@pytest.mark.backend("postgres")
class TestPerformance:
    """One environment per class, one deployment per test case."""

    profile = os.environ.get("PERF_TEST__PROFILE", "dynamic")  # key of PROFILES; sizes the deployment
    sizing: dict[str, Any]
    dags: dict[str, SerializedDAG] = {}
    # Keyed by run id prefix, printed side by side once every test case has run.
    results: dict[str, dict[str, Any]] = {}

    @classmethod
    def teardown_class(cls):
        if cls.results:
            print_to_terminal(f"\n\n{format_summary(cls.results)}\n")

    @classmethod
    def setup_class(cls):
        if cls.profile not in PROFILES:
            pytest.fail(f"profile {cls.profile!r} is not one of {sorted(PROFILES)}")
        cls.sizing = PROFILES[cls.profile]
        log.info("profile %s: %s", cls.profile, cls.sizing)

        wait_for_otel_collector(OTEL_COLLECTOR_HOST, OTEL_COLLECTOR_PORT)
        # Exceeding the limit kills the scheduler mid-run.
        ensure_postgres_max_connections(compute_required_db_connections(cls.sizing))

        # The pytest plugin strips AIRFLOW__* variables; scheduler and API server must share the secret.
        os.environ["AIRFLOW__API_AUTH__JWT_SECRET"] = "test-secret-key-for-testing"
        os.environ["AIRFLOW__API_AUTH__JWT_ISSUER"] = "airflow"

        os.environ["AIRFLOW__CORE__EXECUTOR"] = "CeleryExecutor"
        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = DAG_FOLDER
        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = "/dev/null"
        os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"

        os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE"] = str(DB_POOL_SIZE)
        os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW"] = str(DB_MAX_OVERFLOW)
        os.environ["AIRFLOW__CELERY__RESULT_BACKEND_SQLALCHEMY_ENGINE_OPTIONS"] = json.dumps(
            {"pool_size": RESULT_BACKEND_POOL_SIZE, "max_overflow": RESULT_BACKEND_MAX_OVERFLOW}
        )
        # The default spawns a ProcessPoolExecutor per publish batch, on the scheduler's heartbeat.
        os.environ["AIRFLOW__CELERY__SYNC_PARALLELISM"] = "1"
        # The default of one second fails publishes behind a busy broker.
        os.environ["AIRFLOW__CELERY__OPERATION_TIMEOUT"] = "300"

        # Before db migrate, which creates the default pool with this slot count.
        export_setting("AIRFLOW__CORE__PARALLELISM", cls.sizing["parallelism"])
        export_setting(
            "AIRFLOW__CORE__DEFAULT_POOL_TASK_SLOT_COUNT", cls.sizing["default_pool_task_slot_count"]
        )
        export_setting("AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG", cls.sizing["max_active_tasks_per_dag"])
        export_setting("AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG", cls.sizing["max_active_runs_per_dag"])
        export_setting("AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY", cls.sizing["max_tis_per_query"])

        os.environ["AIRFLOW__METRICS__OTEL_ON"] = "True"
        os.environ["AIRFLOW__METRICS__OTEL_HOST"] = OTEL_COLLECTOR_HOST
        os.environ["AIRFLOW__METRICS__OTEL_PORT"] = str(OTEL_COLLECTOR_PORT)
        # breeze's OTEL_METRIC_EXPORT_INTERVAL=30000 overrides the Airflow option; Prometheus still
        # scrapes every 30s.
        os.environ["AIRFLOW__METRICS__OTEL_INTERVAL_MILLISECONDS"] = "1000"
        os.environ["OTEL_METRIC_EXPORT_INTERVAL"] = "1000"
        # The dashboard reads the tagged names; legacy names add one series family per task.
        os.environ["AIRFLOW__METRICS__LEGACY_NAMES_ON"] = "False"
        os.environ["AIRFLOW__TRACES__OTEL_ON"] = "True"
        os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "http/protobuf"
        os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = (
            f"http://{OTEL_COLLECTOR_HOST}:{OTEL_COLLECTOR_PORT}/v1/traces"
        )

        # Once per class: the scheduler ignores finished runs, and run ids carry the test case.
        subprocess.run(["airflow", "db", "reset", "--yes"], check=True, env=os.environ.copy())
        subprocess.run(["airflow", "db", "migrate"], check=True, env=os.environ.copy())

        # Every Dag in the folder; the test picks by id.
        cls.dags = serialize_and_get_dags(dag_folder=DAG_FOLDER)
        assert cls.dags, f"no Dags found in {DAG_FOLDER}"

    def start_usage_poller(self, processes: dict[str, subprocess.Popen]) -> subprocess.Popen:
        """Start poll_usage.py, which pushes CPU, memory and worker slot gauges for ``processes``."""
        slot_budget = self.sizing["worker_count"] * self.sizing["worker_concurrency"]
        command = [sys.executable, POLL_USAGE_SCRIPT, "--slot-budget", str(slot_budget)]
        for role, process in processes.items():
            command.append(f"{role}={process.pid}")
        # Own process group, or terminate_processes would kill pytest with it.
        return start_process(command)

    def execute_dag_runs(
        self,
        *,
        run_id_prefix: str,
        dag_ids: list[str],
        dag_run_count: int,
        run_conf: dict[str, Any],
        cutoff_seconds: int,
    ) -> None:
        """Create the Dag runs, start the processes, wait for the runs or the cutoff, stop the processes."""
        dags = {dag_id: self.dags[dag_id] for dag_id in dag_ids}
        # Created before any process exists, so every test case starts from the same rows, all
        # eligible at once, and CLI triggering cannot cap the live runs at its own rate. This is the
        # scheduler under full load, not a production trickle.
        runs = create_queued_dag_runs(
            dags, run_id_prefix=run_id_prefix, dag_run_count=dag_run_count, run_conf=run_conf
        )
        # Workers would otherwise start on tasks queued by an earlier test case or session.
        flush_celery_broker()
        # Inherited by every process started below; the dashboard and the summary group by it.
        os.environ["OTEL_RESOURCE_ATTRIBUTES"] = f"perf.arm={run_id_prefix}"
        processes, started_at = start_processes(
            worker_count=self.sizing["worker_count"],
            worker_concurrency=self.sizing["worker_concurrency"],
            api_server_workers=self.sizing["api_server_workers"],
        )
        try:
            processes["usage_poller"] = self.start_usage_poller(processes)
            # Runs still going at the cutoff do not fail the test case; it measures the window.
            with ProgressReporter(runs):
                states = wait_for_dag_runs(runs, max_wait_time=cutoff_seconds)
            elapsed_seconds = time.monotonic() - started_at

            exited = {}
            for role, process in processes.items():
                if process.poll() is not None:
                    exited[role] = process.returncode
            assert not exited, f"processes exited, with these codes: {exited}"

            failed = {run: state for run, state in states.items() if state == State.FAILED}
            assert not failed, f"{len(failed)} of {len(runs)} Dag runs failed: {failed}"
        finally:
            terminate_processes(processes)
            # The scheduler of the next test case would pick them up.
            deleted = delete_unfinished_dag_runs(runs)
        succeeded = sum(state == State.SUCCESS for state in states.values())
        # After the processes are stopped, so their shutdown logging does not bury it.
        print_to_terminal(
            f"\n{run_id_prefix}: {succeeded} of {len(runs)} Dag runs succeeded in {elapsed_seconds:.0f}s, "
            f"{deleted} unfinished deleted\n"
        )
        time.sleep(FINAL_SCRAPE_WAIT_SECONDS)
        self.results[run_id_prefix] = {
            "runs": len(runs),
            "succeeded": succeeded,
            "elapsed": elapsed_seconds,
            "mean_dag_run_seconds": compute_mean_dag_run_seconds(runs),
            **query_prometheus_totals(
                run_id_prefix, int(time.monotonic() - started_at) + FINAL_SCRAPE_WAIT_SECONDS
            ),
        }

    @pytest.mark.parametrize(
        ("test_case", "dag_id", "new_code"),
        [
            # The same Dag: the code under test is what differs between the test cases.
            pytest.param("original_code", "perf_task_group_dag", False, id="original_code"),
            pytest.param("new_code", "perf_task_group_dag", True, id="new_code"),
        ],
    )
    def test_scheduler_load(self, test_case: str, dag_id: str, new_code: bool):
        """Run the same load on perf_task_group_dag, once on the original code and once on the new code."""
        # Inherited by every process started below.
        os.environ[NEW_CODE_ENV_VAR] = str(new_code).lower()

        # The load; each variable overrides its default for one run.
        # Few enough that the new code finishes them, so the time to finish compares the test cases.
        dag_run_count = int(os.environ.get("PERF_TEST__DAG_RUN_COUNT", "5"))  # queued runs created up front
        mapped_task_count = int(os.environ.get("PERF_TEST__MAPPED_TASK_COUNT", "70"))  # instances per stage
        # Stop the test case after this, finished or not.
        cutoff_seconds = int(os.environ.get("PERF_TEST__CUTOFF_SECONDS", "4500"))
        print_to_terminal(
            f"\n\n{test_case}: {dag_run_count} runs of {dag_id}, {mapped_task_count} mapped instances per stage, "
            f"cutoff {cutoff_seconds}s, profile {self.profile}, new code {new_code}\n"
        )

        self.execute_dag_runs(
            # Run ids carry the test case name.
            run_id_prefix=test_case,
            dag_ids=[dag_id],
            dag_run_count=dag_run_count,
            run_conf={"mapped_task_count": mapped_task_count},
            cutoff_seconds=cutoff_seconds,
        )
