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
"""Shared helpers for the duplicate-dispatch integration tests."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from sqlalchemy import select

from airflow.models.dagrun import DagRun
from airflow.models.log import Log
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session

log = logging.getLogger(__name__)

BANNER_WIDTH = 90


def print_banner(text: str) -> None:
    print(f"\n{'=' * BANNER_WIDTH}\n{text}\n{'=' * BANNER_WIDTH}\n")


def get_state_mismatch_logs(dag_id: str, run_id: str) -> list[tuple]:
    with create_session() as session:
        return list(
            session.execute(
                select(Log.dttm, Log.extra)
                .where(Log.dag_id == dag_id, Log.run_id == run_id, Log.event == "state mismatch")
                .order_by(Log.dttm)
            ).all()
        )


def print_incident_summary(*, dag_id: str, task_id: str, run_id: str, marker_file: str) -> None:
    """Print a numbered narrative of the duplicate-dispatch race assembled from the DB."""
    with create_session() as session:
        ti = session.scalar(
            select(TaskInstance).where(
                TaskInstance.task_id == task_id,
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == run_id,
            )
        )
        dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id))
        running_rows = list(
            session.execute(
                select(Log.dttm, Log.extra)
                .where(Log.dag_id == dag_id, Log.run_id == run_id, Log.event == "running")
                .order_by(Log.dttm)
            ).all()
        )
    mismatch_rows = get_state_mismatch_logs(dag_id, run_id)
    marker = Path(marker_file).read_text() if os.path.exists(marker_file) else None

    lines = [
        "DUPLICATE-DISPATCH INCIDENT SUMMARY",
        f"  dag_id={dag_id} task_id={task_id} run_id={run_id}",
        "",
        f"  1. Task instance queued at {ti.queued_dttm} by scheduler job {ti.queued_by_job_id}.",
    ]

    if marker:
        lines.append(f"  2. The workload was dispatched TWICE (duplicate marker: {marker}).")
    else:
        lines.append("  2. NO duplicate dispatch happened — the scenario was NOT exercised!")

    if running_rows:
        dttm, extra = running_rows[0]
        hostname = None
        if extra:
            try:
                hostname = json.loads(extra).get("host_name")
            except (ValueError, AttributeError):
                hostname = extra
        lines.append(
            f"  3. The race WINNER marked the task RUNNING at {dttm} (hostname: {hostname}); "
            "the LOSER's identical start call got 409 Conflict and died "
            "(its TaskAlreadyRunningError traceback appears earlier in the output)."
        )
    else:
        lines.append("  3. The task instance never reached RUNNING.")

    if mismatch_rows:
        dttm, extra = mismatch_rows[0]
        lines.extend(
            [
                f"  4. BUG REPRODUCED at {dttm}: the scheduler trusted the dead LOSER's FAILED",
                "     event and killed the task instance the WINNER was still running:",
                f"       {extra}",
                "  5. Collateral damage: the healthy WINNER was told to terminate (409 not_running)",
                f"     and SIGKILLed. Final states: ti={ti.state!r} (ended {ti.end_date}), "
                f"dag_run={dag_run.state!r}.",
            ]
        )
    else:
        lines.extend(
            [
                "  4. The scheduler did NOT act on any stale FAILED event for the running task",
                "     (no 'state mismatch' log row) — the winner was left alone.",
                f"  5. Final states: ti={ti.state!r} (ended {ti.end_date}), dag_run={dag_run.state!r}.",
            ]
        )

    print_banner("\n".join(lines))


def print_task_logs(dag_id: str, run_id: str) -> None:
    # Structured log layout: /root/airflow/logs/dag_id=.../run_id=.../task_id=.../attempt=1.log
    logs_root = os.path.join("/root/airflow/logs", f"dag_id={dag_id}", f"run_id={run_id}")
    for root, _dirs, files in os.walk(logs_root):
        for filename in files:
            if filename.endswith(".log"):
                full_path = os.path.join(root, filename)
                print(f"\n===== TASK LOG FILE: {full_path} - START =====\n")
                try:
                    with open(full_path) as f:
                        print(f.read())
                except Exception as e:
                    log.error("Could not read %s: %s", full_path, e)
                print("\n===== END =====\n")
