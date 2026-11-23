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
from __future__ import annotations

from datetime import datetime
from typing import Any

from airflow.sdk import DAG, task, task_group

STAGE_COUNT = 9


@task
def get_mapped_task_indexes(**context: Any) -> list[int]:
    """Return [0, ..., (mapped_task_count - 1)], one element per mapped copy of the stage group."""
    # The run's conf overrides the Dag's params; the test sets the count there.
    return list(range(int(context["params"]["mapped_task_count"])))


@task
def run_stage(parameter: int) -> None:
    """Do nothing."""
    # Empty on purpose: any work here would measure the workers, not the scheduler.


@task_group
def run_stages(parameter: int) -> None:
    previous = None
    for stage in range(1, STAGE_COUNT + 1):
        current = run_stage.override(task_id=f"stage_{stage}")(parameter=parameter)
        # Each stage instance waits only for the previous stage's instance in the same copy.
        if previous is not None:
            previous >> current
        previous = current


with DAG(
    dag_id="perf_task_group_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0},
    params={"mapped_task_count": 20},  # overridden by each run's conf
    tags=["perf"],
):
    # Same stages and sizes as perf_dag, but the group expands once, so every stage's copies exist
    # as soon as the list is ready, instead of each stage expanding when the previous one finishes.
    run_stages.expand(parameter=get_mapped_task_indexes())
