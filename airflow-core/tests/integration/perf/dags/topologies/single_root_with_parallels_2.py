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

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

"""
Single root with parallels DAG: level 0 (root task) -> level 1 (100 parallel tasks).

0 -> (1_1, 1_2, 1_3, 1_4, ..., 1_100 -- parallel)
"""

DEFAULT_ARGS = {
    "owner": "test",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="single_root_with_parallels_2",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    max_active_runs=5,
) as dag:
    # Root
    start = BashOperator(
        task_id="task__0",
        bash_command='echo "Single root with parallels DAG -- START (root task__0)" && sleep 7',
    )

    # Level 1: 100 parallel tasks
    level1_tasks = []
    for i in range(1, 101):
        t = BashOperator(
            task_id=f"task__1_{i}",
            bash_command=f'echo "Single root with parallels DAG -- Executing level 1, task task__1_{i}, leaf {i}" && sleep 3',
        )
        level1_tasks.append(t)

    # Wiring: task__0 -> all task__1_*
    start >> level1_tasks
