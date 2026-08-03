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
Microbenchmark: is the FinishedTI path slower to CONSUME in _get_ready_tis, or was the
+35% per-call python time in the A/B runs pure CPU contention from saturated workers?

Runs task_instance_scheduling_decisions on an idle box against a fixed mid-run DB state
(one stage schedulable, six finished), interleaving flag off/on blocks. Fresh session per
iteration so ORM hydration is paid every call, as it is in the scheduler.
"""

from __future__ import annotations

import os
import statistics
import time

from sqlalchemy import select, update

from airflow.models import DagRun, TaskInstance as TI
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.session import create_session

DAG_ID = "process_region1"
FLAG = "AIRFLOW__SCHEDULER__NEW_TI_RESCAN_FEATURE"
WARMUP, ITERS = 20, 150

grt_times: list[float] = []
_orig_grt = DagRun._get_ready_tis


def _timed_grt(self, *args, **kwargs):
    t0 = time.monotonic()
    try:
        return _orig_grt(self, *args, **kwargs)
    finally:
        grt_times.append(time.monotonic() - t0)


DagRun._get_ready_tis = _timed_grt


def stage_db_state() -> None:
    with create_session() as session:
        prefix = "scan_capture_verify_and_report."
        session.execute(
            update(TI)
            .where(TI.dag_id == DAG_ID, TI.task_id == f"{prefix}wait_for_isilon_sync")
            .values(state="scheduled")
            .execution_options(synchronize_session=False)
        )
        session.execute(
            update(TI)
            .where(
                TI.dag_id == DAG_ID,
                TI.task_id.in_([f"{prefix}DELETE_RAW", f"{prefix}UPDATE_ENTITIES_STATE"]),
            )
            .values(state=None)
            .execution_options(synchronize_session=False)
        )
        session.commit()


def run_block(flag_value: str) -> tuple[float, float]:
    os.environ[FLAG] = flag_value
    totals: list[float] = []
    grt_times.clear()
    for i in range(WARMUP + ITERS):
        with create_session() as session:
            dr = session.scalar(select(DagRun).where(DagRun.dag_id == DAG_ID))
            dr.dag = SerializedDagModel.get(DAG_ID, session=session).dag
            t0 = time.monotonic()
            dr.task_instance_scheduling_decisions(session=session)
            dt = time.monotonic() - t0
            session.rollback()
        if i >= WARMUP:
            totals.append(dt)
        elif grt_times:
            grt_times.clear()
    return statistics.mean(totals) * 1000, statistics.mean(grt_times[-ITERS:]) * 1000


def main() -> None:
    stage_db_state()
    print(f"{'block':<10}{'flag':>6}{'total ms':>10}{'_get_ready_tis ms':>19}")
    results: dict[str, list[tuple[float, float]]] = {"False": [], "True": []}
    for block, flag in enumerate(["False", "True", "False", "True"]):
        total_ms, grt_ms = run_block(flag)
        results[flag].append((total_ms, grt_ms))
        print(f"{block + 1:<10}{flag:>6}{total_ms:>10.2f}{grt_ms:>19.2f}")
    for flag, vals in results.items():
        t = statistics.mean(v[0] for v in vals)
        g = statistics.mean(v[1] for v in vals)
        print(f"flag={flag:<6} mean total={t:.2f}ms  mean _get_ready_tis={g:.2f}ms")


if __name__ == "__main__":
    main()
