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
Shared definition for the ``process_regionN`` dags.

Reproduces a production workload: N identical per-region dags, each with a deferred
parameter task feeding a chain of mapped stages. Every stage expands to
``MAPPED_TASK_COUNT`` instances and depends on the *whole* previous stage, so each
stage boundary is a full barrier -- the shape that makes TriggerRuleDep's aggregate
upstream counting expensive and keeps the scheduler resolving dependencies inside the
transaction that holds ``FOR UPDATE`` on the dag_run rows.

Total mapped task instances = number of region dags * len(STAGES) * MAPPED_TASK_COUNT.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger
from airflow.sdk import DAG, BaseOperator, TaskGroup, task

if TYPE_CHECKING:
    from airflow.sdk import Context

NUM_REGIONS = 35

# The test is the source of truth and exports this before starting any process; the
# default only applies when the dags are parsed outside the test. 35 * 9 * 19 = 5985.
MAPPED_TASK_COUNT = int(os.environ.get("PERF_GROSS_MAPPED_COUNT", "19"))

# Stands in for the external pipeline the real get_capture_tasks_parameters waits on.
CAPTURE_PARAMS_DEFER_SECONDS = int(os.environ.get("PERF_GROSS_DEFER_SECONDS", "60"))

STAGES = (
    "SCAN_TOPIC",
    "get_variations",
    "should_run_capture",
    "CAPTURE_RAW",
    "CONVERT",
    "VERIFY_OUTPUT",
    "wait_for_isilon_sync",
    "DELETE_RAW",
    "UPDATE_ENTITIES_STATE",
)

DEFAULT_ARGS = {
    "owner": "test",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


class CaptureParametersOperator(BaseOperator):
    """Defer for a fixed delay, then emit the list that every mapped stage expands over."""

    def __init__(self, *, defer_seconds: int, param_count: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self.defer_seconds = defer_seconds
        self.param_count = param_count

    def execute(self, context: Context) -> None:
        self.defer(
            trigger=TimeDeltaTrigger(timedelta(seconds=self.defer_seconds)),
            method_name="build_parameters",
        )

    def build_parameters(self, context: Context, event: Any) -> list[int]:
        return list(range(self.param_count))


@task
def run_stage(param: int) -> None:
    """Do nothing: real work would turn this into a worker-throughput test."""


def build_region_dag(region: int) -> DAG:
    with DAG(
        dag_id=f"process_region{region}",
        default_args=DEFAULT_ARGS,
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["gross"],
    ) as dag:
        parameters = CaptureParametersOperator(
            task_id="get_capture_tasks_parameters",
            defer_seconds=CAPTURE_PARAMS_DEFER_SECONDS,
            param_count=MAPPED_TASK_COUNT,
        ).output

        with TaskGroup(group_id="scan_capture_verify_and_report"):
            previous = None
            for stage in STAGES:
                # Expanding over `parameters` also makes get_capture_tasks_parameters an
                # upstream of every stage, matching the fan-out seen in the client's graph.
                current = run_stage.override(task_id=stage).expand(param=parameters)
                if previous is not None:
                    previous >> current
                previous = current

    return dag
