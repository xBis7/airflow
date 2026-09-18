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
Seed Dag run history for the UX demo Dags.

Runs inside the breeze container at ``breeze start-airflow`` time, after the database is
migrated and before any Airflow component starts. Every run replaces all runs of the demo
Dags with history generated relative to the current time. See ``dev/ux_demo/README.md``.
"""

from __future__ import annotations

import hashlib
import math
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy import delete, func, select

from airflow._shared.timezones import timezone
from airflow.dag_processing.bundles.base import unpack_bundle_version
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.dagbag import BundleDagBag, sync_bag_to_db
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.tasklog import LogTemplate
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.orm import Session

    from airflow.serialization.definitions.dag import SerializedDAG

BUNDLE_NAME = "dags-folder"
DEMO_DAGS_SUBFOLDER = "ux_demo"
PROFILES = ("small", "full")

# Keep in sync with the task duration range in dags/ux_demo_dags.py so that live runs
# and seeded runs look alike in the duration charts.
MIN_TASK_SECONDS = 2.0
MAX_TASK_SECONDS = 8.0

INCIDENT_FAILURE_RATE = 0.9
RUN_CHUNK_SIZE = 2000
TI_CHUNK_SIZE = 5000


@dataclass(frozen=True)
class SeedPlan:
    dag_id: str
    pattern: Literal["organic", "manual", "showcase", "empty"]
    # Paused Dags keep their seeded running/queued runs frozen: the scheduler ignores them.
    paused: bool
    history_days: dict[str, int]
    base_failure_rate: float = 0.0
    weekly_incident_probability: float = 0.0
    incident_hours: tuple[float, float] = (0.0, 0.0)
    monthly_gap_probability: float = 0.0
    gap_hours: tuple[float, float] = (0.0, 0.0)


SEED_PLANS = [
    SeedPlan(
        dag_id="ux_demo_hourly_etl",
        pattern="organic",
        paused=False,
        history_days={"small": 14, "full": 400},
        base_failure_rate=0.02,
        weekly_incident_probability=0.35,
        incident_hours=(3, 30),
        monthly_gap_probability=0.3,
        gap_hours=(6, 60),
    ),
    SeedPlan(
        dag_id="ux_demo_frequent_sync",
        pattern="organic",
        paused=False,
        history_days={"small": 3, "full": 45},
        base_failure_rate=0.03,
        weekly_incident_probability=0.6,
        incident_hours=(1, 8),
        monthly_gap_probability=0.5,
        gap_hours=(3, 20),
    ),
    SeedPlan(
        dag_id="ux_demo_daily_report",
        pattern="organic",
        paused=False,
        history_days={"small": 60, "full": 800},
        base_failure_rate=0.03,
        weekly_incident_probability=0.12,
        incident_hours=(48, 120),
        monthly_gap_probability=0.1,
        gap_hours=(72, 240),
    ),
    SeedPlan(
        dag_id="ux_demo_weekday_export",
        pattern="organic",
        paused=False,
        history_days={"small": 60, "full": 500},
        base_failure_rate=0.05,
        weekly_incident_probability=0.1,
        incident_hours=(24, 72),
    ),
    SeedPlan(
        dag_id="ux_demo_monthly_billing",
        pattern="organic",
        paused=False,
        history_days={"small": 365, "full": 1100},
        base_failure_rate=0.1,
    ),
    SeedPlan(
        dag_id="ux_demo_adhoc_reprocess",
        pattern="manual",
        paused=False,
        history_days={"small": 30, "full": 180},
        base_failure_rate=0.15,
    ),
    SeedPlan(
        dag_id="ux_demo_state_showcase",
        pattern="showcase",
        paused=True,
        history_days={"small": 5, "full": 21},
    ),
    SeedPlan(
        dag_id="ux_demo_new_pipeline",
        pattern="empty",
        paused=True,
        history_days={"small": 0, "full": 0},
    ),
]


@dataclass(frozen=True)
class RunSpec:
    logical_date: datetime
    run_after: datetime
    data_interval: tuple[datetime, datetime]
    run_type: DagRunType
    state: DagRunState


def get_stable_roll(*parts: object) -> float:
    """
    Return a number in [0, 1) that depends only on ``parts``.

    Hashing the run's identity instead of drawing from a sequence keeps every run's outcome
    the same across re-seeds, even though the seeded window moves with the current time.
    """
    digest = hashlib.sha256("|".join(str(part) for part in parts).encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


@cache
def get_window(
    dag_id: str, kind: str, anchor: datetime, probability: float, anchor_span_hours: float, hours: tuple
) -> tuple[datetime, datetime] | None:
    """Return the incident/gap window that starts within the period beginning at ``anchor``, if any."""
    if get_stable_roll(dag_id, kind, anchor, "exists") >= probability:
        return None
    start = anchor + timedelta(hours=anchor_span_hours * get_stable_roll(dag_id, kind, anchor, "start"))
    min_hours, max_hours = hours
    length = min_hours + (max_hours - min_hours) * get_stable_roll(dag_id, kind, anchor, "length")
    return start, start + timedelta(hours=length)


def is_in_window(plan: SeedPlan, kind: Literal["incident", "gap"], moment: datetime) -> bool:
    midnight = moment.replace(hour=0, minute=0, second=0, microsecond=0)
    if kind == "incident":
        this_anchor = midnight - timedelta(days=moment.weekday())
        previous_anchor = this_anchor - timedelta(days=7)
        probability, span_hours, hours = plan.weekly_incident_probability, 7 * 24.0, plan.incident_hours
    else:
        this_anchor = midnight.replace(day=1)
        previous_anchor = (this_anchor - timedelta(days=1)).replace(day=1)
        probability, span_hours, hours = plan.monthly_gap_probability, 27 * 24.0, plan.gap_hours
    if probability <= 0:
        return False
    # A window can spill over into the next period, so the previous period's window counts too.
    for anchor in (this_anchor, previous_anchor):
        window = get_window(plan.dag_id, kind, anchor, probability, span_hours, hours)
        if window and window[0] <= moment < window[1]:
            return True
    return False


def get_organic_state(plan: SeedPlan, logical_date: datetime) -> DagRunState | None:
    if is_in_window(plan, "gap", logical_date):
        return None
    failure_rate = (
        INCIDENT_FAILURE_RATE if is_in_window(plan, "incident", logical_date) else plan.base_failure_rate
    )
    if get_stable_roll(plan.dag_id, logical_date, "state") < failure_rate:
        return DagRunState.FAILED
    return DagRunState.SUCCESS


def get_showcase_state(logical_date: datetime, now: datetime) -> DagRunState | None:
    """
    Return the state for a run of the paused "test card" Dag (4 runs per hour).

    The last three hour buckets show the in-flight mixes; older history repeats 4-hour bands of
    pure success, pure failure, even mix, mostly success, every-other-day gaps and half-density.
    """
    slot = logical_date.minute // 15
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    run_hour = logical_date.replace(minute=0, second=0, microsecond=0)
    hours_ago = int((current_hour - run_hour).total_seconds() // 3600)
    if hours_ago == 0:
        return DagRunState.RUNNING if slot == 0 else DagRunState.QUEUED
    if hours_ago == 1:
        return (DagRunState.FAILED, DagRunState.FAILED, DagRunState.RUNNING, DagRunState.RUNNING)[slot]
    if hours_ago == 2:
        return DagRunState.RUNNING if slot == 3 else DagRunState.SUCCESS

    band = logical_date.hour // 4
    if band == 1:
        return DagRunState.FAILED
    if band == 2:
        return DagRunState.FAILED if slot % 2 else DagRunState.SUCCESS
    if band == 3:
        return DagRunState.FAILED if slot == 3 else DagRunState.SUCCESS
    if band == 4 and logical_date.toordinal() % 2 == 0:
        return None
    if band == 5 and slot % 2:
        return None
    return DagRunState.SUCCESS


def iter_scheduled_run_specs(
    plan: SeedPlan, dag: SerializedDAG, earliest: datetime, now: datetime
) -> Iterator[RunSpec]:
    for info in dag.iter_dagrun_infos_between(earliest, now):
        if info.logical_date is None or info.data_interval is None:
            continue
        if plan.pattern == "showcase":
            state = get_showcase_state(info.logical_date, now)
        else:
            state = get_organic_state(plan, info.logical_date)
        if state is None:
            continue
        yield RunSpec(
            logical_date=info.logical_date,
            run_after=info.run_after,
            data_interval=(info.data_interval.start, info.data_interval.end),
            run_type=DagRunType.SCHEDULED,
            state=state,
        )


def iter_manual_run_specs(plan: SeedPlan, earliest: datetime, now: datetime) -> Iterator[RunSpec]:
    """Yield bursts of manual runs: most days have none, some have several within a few hours."""
    day = earliest.replace(hour=0, minute=0, second=0, microsecond=0)
    while day <= now:
        if get_stable_roll(plan.dag_id, day, "burst") < 0.22:
            burst_size = 1 + int(7 * get_stable_roll(plan.dag_id, day, "size") ** 2)
            burst_start = day + timedelta(hours=8 + 8 * get_stable_roll(plan.dag_id, day, "hour"))
            moments = {
                (burst_start + timedelta(minutes=150 * get_stable_roll(plan.dag_id, day, index))).replace(
                    microsecond=0
                )
                for index in range(burst_size)
            }
            for moment in sorted(moments):
                failed = get_stable_roll(plan.dag_id, moment, "state") < plan.base_failure_rate
                yield RunSpec(
                    logical_date=moment,
                    run_after=moment,
                    data_interval=(moment, moment),
                    run_type=DagRunType.MANUAL,
                    state=DagRunState.FAILED if failed else DagRunState.SUCCESS,
                )
        day += timedelta(days=1)


def compute_stages(dag: SerializedDAG) -> list[list[str]]:
    """Group task ids by their depth in the Dag; tasks in one stage run in parallel."""
    levels: dict[str, int] = {}

    def get_level(task_id: str) -> int:
        if task_id not in levels:
            upstream = dag.task_dict[task_id].upstream_task_ids
            levels[task_id] = 1 + max((get_level(up) for up in upstream), default=-1)
        return levels[task_id]

    stages: dict[int, list[str]] = {}
    for task_id in sorted(dag.task_dict):
        stages.setdefault(get_level(task_id), []).append(task_id)
    return [stages[level] for level in sorted(stages)]


def get_task_seconds(dag_id: str, task_id: str, logical_date: datetime) -> float:
    seconds = MIN_TASK_SECONDS + (MAX_TASK_SECONDS - MIN_TASK_SECONDS) * get_stable_roll(
        dag_id, task_id, logical_date, "duration"
    )
    slow_drift = 1 + 0.35 * math.sin(logical_date.toordinal() / 9.0)
    outlier = 3.0 if get_stable_roll(dag_id, task_id, logical_date, "outlier") < 0.02 else 1.0
    return seconds * slow_drift * outlier


def build_run_rows(
    spec: RunSpec,
    *,
    dag_id: str,
    stages: list[list[str]],
    ti_templates: dict[str, dict[str, Any]],
    run_id: str,
) -> tuple[datetime | None, datetime | None, list[dict[str, Any]]]:
    """Return the run's start date, end date and task instance rows, consistent with its state."""
    queued = spec.state == DagRunState.QUEUED
    run_start = None if queued else spec.run_after + timedelta(seconds=1.5)
    # The stage where a failed run breaks, or where a running run currently is.
    pivot = int(len(stages) * get_stable_roll(dag_id, spec.logical_date, "pivot"))
    cursor = run_start
    rows = []
    for index, stage in enumerate(stages):
        culprit = stage[int(len(stage) * get_stable_roll(dag_id, spec.logical_date, "culprit"))]
        stage_end = cursor
        for task_id in stage:
            row = dict(
                ti_templates[task_id],
                run_id=run_id,
                state=None,
                start_date=None,
                end_date=None,
                duration=None,
                queued_dttm=None,
                scheduled_dttm=None,
                try_number=0,
            )
            rows.append(row)
            reached = spec.state == DagRunState.SUCCESS or index <= pivot
            if cursor is None or not reached:
                if spec.state == DagRunState.FAILED:
                    row.update(
                        state=TaskInstanceState.UPSTREAM_FAILED,
                        start_date=cursor,
                        end_date=cursor,
                        duration=0,
                    )
                continue

            start = cursor + timedelta(seconds=1)
            seconds = get_task_seconds(dag_id, task_id, spec.logical_date)
            row.update(
                scheduled_dttm=cursor,
                queued_dttm=cursor + timedelta(seconds=0.3),
                start_date=start,
                try_number=1,
                hostname="ux-demo-worker",
            )
            if index == pivot and spec.state == DagRunState.RUNNING:
                row.update(state=TaskInstanceState.RUNNING)
                continue
            if index == pivot and spec.state == DagRunState.FAILED and task_id == culprit:
                seconds *= 0.2 + 0.8 * get_stable_roll(dag_id, task_id, spec.logical_date, "failed_at")
                row.update(state=TaskInstanceState.FAILED)
            else:
                row.update(state=TaskInstanceState.SUCCESS)
            end = start + timedelta(seconds=seconds)
            row.update(end_date=end, duration=seconds)
            stage_end = end if stage_end is None else max(stage_end, end)
        cursor = stage_end

    finished = spec.state in (DagRunState.SUCCESS, DagRunState.FAILED)
    run_end = cursor + timedelta(seconds=0.5) if finished and cursor else None
    return run_start, run_end, rows


def install_and_parse_demo_dags() -> None:
    """
    Copy the demo Dags into the Dags folder and serialize them.

    The Dags folder (``files/dags``) is gitignored, so the Dags live next to this script. Only
    the demo Dags are parsed, so startup time does not depend on what else is in the folder.
    """
    manager = DagBundlesManager()
    # The bundle rows must be committed before Dags referencing them are written.
    with create_session() as session:
        manager.sync_bundles_to_db(session=session)
    bundle = manager.get_bundle(BUNDLE_NAME)
    bundle.initialize()
    demo_dags_path = Path(bundle.path) / DEMO_DAGS_SUBFOLDER
    shutil.rmtree(demo_dags_path, ignore_errors=True)
    shutil.copytree(
        Path(__file__).parent / "dags", demo_dags_path, ignore=shutil.ignore_patterns("__pycache__")
    )
    dag_bag = BundleDagBag(demo_dags_path, bundle_path=bundle.path, bundle_name=bundle.name)
    if dag_bag.import_errors:
        raise RuntimeError(f"The demo Dags failed to import: {dag_bag.import_errors}")
    version, version_data = unpack_bundle_version(bundle.get_current_version(), bundle)
    with create_session() as session:
        sync_bag_to_db(
            dag_bag, bundle.name, bundle_version=version, version_data=version_data, session=session
        )


def seed_dag(plan: SeedPlan, *, profile: str, now: datetime, log_template_id: int, session: Session) -> int:
    dag_version = DagVersion.get_latest_version(plan.dag_id, load_serialized_dag=True, session=session)
    dag_model = session.get(DagModel, plan.dag_id)
    if dag_version is None or dag_model is None:
        raise RuntimeError(f"Dag {plan.dag_id} is not serialized; did the demo Dags get parsed?")
    dag = dag_version.serialized_dag.dag
    dag_model.is_paused = plan.paused

    earliest = now - timedelta(days=plan.history_days[profile])
    if plan.pattern == "empty":
        specs: Iterator[RunSpec] = iter(())
    elif plan.pattern == "manual":
        specs = iter_manual_run_specs(plan, earliest, now)
    else:
        specs = iter_scheduled_run_specs(plan, dag, earliest, now)

    stages = compute_stages(dag)
    ti_templates: dict[str, dict[str, Any]] = {}
    runs: list[DagRun] = []
    ti_rows: list[dict[str, Any]] = []
    last_scheduled_run: DagRun | None = None
    run_count = 0

    def flush_pending() -> None:
        session.add_all(runs)
        session.flush()
        session.bulk_insert_mappings(TaskInstance.__mapper__, ti_rows, render_nulls=True)
        runs.clear()
        ti_rows.clear()

    for spec in specs:
        manual = spec.run_type == DagRunType.MANUAL
        run = DagRun(
            dag_id=plan.dag_id,
            run_id=DagRun.generate_run_id(
                run_type=spec.run_type, logical_date=spec.logical_date, run_after=spec.run_after
            ),
            logical_date=spec.logical_date,
            run_after=spec.run_after,
            data_interval=spec.data_interval,
            run_type=spec.run_type,
            state=spec.state,
            queued_at=spec.run_after + timedelta(seconds=0.5),
            triggered_by=DagRunTriggeredByType.UI if manual else DagRunTriggeredByType.TIMETABLE,
            triggering_user_name="admin" if manual else None,
        )
        if not ti_templates:
            ti_templates.update(
                {
                    task_id: TaskInstance.insert_mapping(
                        run.run_id, task, map_index=-1, dag_version_id=dag_version.id, dag_run=run
                    )
                    for task_id, task in dag.task_dict.items()
                }
            )
        run_start, run_end, rows = build_run_rows(
            spec, dag_id=plan.dag_id, stages=stages, ti_templates=ti_templates, run_id=run.run_id
        )
        # An unpaused Dag must only get finished runs: the scheduler would pick up anything else.
        if not plan.paused and (run_end is None or run_end > now):
            continue
        run.start_date = run_start
        run.end_date = run_end
        run.created_dag_version_id = dag_version.id
        run.log_template_id = log_template_id
        runs.append(run)
        ti_rows.extend(rows)
        run_count += 1
        if not manual:
            last_scheduled_run = run
        if len(runs) >= RUN_CHUNK_SIZE or len(ti_rows) >= TI_CHUNK_SIZE:
            flush_pending()
    flush_pending()

    # Let the scheduler continue right after the seeded history.
    dag_model.calculate_dagrun_date_fields(dag, reference_run=last_scheduled_run)
    return run_count


def main() -> None:
    profile = os.environ.get("UX_DEMO_SEED_PROFILE") or "full"
    if profile not in PROFILES:
        raise SystemExit(f"UX_DEMO_SEED_PROFILE must be one of {PROFILES}, got {profile!r}")
    started = time.monotonic()
    now = timezone.utcnow()
    dag_ids = [plan.dag_id for plan in SEED_PLANS]
    print(f"Seeding UX demo data with the '{profile}' profile")

    install_and_parse_demo_dags()

    with create_session() as session:
        deleted = session.execute(delete(DagRun).where(DagRun.dag_id.in_(dag_ids)))
        print(f"  removed {deleted.rowcount} existing runs of the demo Dags")
        log_template_id = session.scalar(select(func.max(LogTemplate.id))) or 0

    for plan in SEED_PLANS:
        dag_started = time.monotonic()
        with create_session() as session:
            run_count = seed_dag(
                plan, profile=profile, now=now, log_template_id=log_template_id, session=session
            )
        state = "paused" if plan.paused else "active"
        print(f"  {plan.dag_id}: {run_count} runs ({state}) in {time.monotonic() - dag_started:.1f}s")

    print(f"UX demo data seeded in {time.monotonic() - started:.1f}s")


if __name__ == "__main__":
    main()
