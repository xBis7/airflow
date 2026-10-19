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
Branching DAG: level 0 (root task) -> level 1 (5 tasks) -> level 2 (15 tasks).


                                                  0
                                                  |
         _______________________________________________________________________________________
        |                    |                    |                     |                       |
       1_1                  1_2                  1_3                   1_4                     1_5
        |                    |                    |                     |                       |
  _____________        _____________        _____________        _______________         _______________
 |      |      |      |      |      |      |      |      |      |       |       |       |       |       |
2_1    2_2    2_3    2_4    2_5    2_6    2_7    2_8    2_9    2_10    2_11    2_12    2_13    2_14    2_15
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
    dag_id="branching_dag_4",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    max_active_runs=5,
) as dag:
    # Root
    start = BashOperator(
        task_id="task__0",
        bash_command='echo "Branching DAG -- START" && sleep 5',
    )

    # Level 1 (5 parallel tasks)
    level1_tasks = []
    for i in range(1, 6):
        t = BashOperator(
            task_id=f"task__1_{i}",
            bash_command=f'echo "Branching DAG -- Executing level 1, task task__1_{i}, branch {i}" && sleep 3',
        )
        level1_tasks.append(t)

    # Level 2 (15 tasks, 3 children per l1_i)
    level2_tasks = []
    for i in range(1, 16):
        t = BashOperator(
            task_id=f"task__2_{i}",
            bash_command=f'echo "Branching DAG -- Executing level 2, task task__2_{i}, leaf task {i}" && sleep 3',
        )
        level2_tasks.append(t)

    end = BashOperator(
        task_id="end",
        bash_command='echo "Branching DAG -- END"',
    )

    # Wiring: start -> all l1
    start >> level1_tasks

    # Wiring: each task__1_i fans out to 3 task__2_* tasks
    for idx, parent in enumerate(level1_tasks):
        children = level2_tasks[idx * 3 : (idx + 1) * 3]
        parent >> children

    # Wiring: all task__2_* -> end
    for t in level2_tasks:
        t >> end
