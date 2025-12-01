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
Fan-out DAG: level 0 (root node) -> level 1 (100 parallel nodes)

              node__0
                 |
      _____________________________________________ ... _____________________________________________
     |        |        |        |        |                           |                          |
 node__1_1  node__1_2 node__1_3 ...   node__1_50                 ... node__1_99             node__1_100
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
    dag_id="single_root_with_parallels",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    max_active_runs=5,
) as dag:
    # Root
    start = BashOperator(
        task_id="node__0",
        bash_command='echo "Fan-out DAG -- START (root node__0)"',
    )

    # Level 1: 100 parallel tasks
    level1_tasks = []
    for i in range(1, 101):
        t = BashOperator(
            task_id=f"node__1_{i}",
            bash_command=f'echo "Fan-out DAG -- Executing level 1, task node__1_{i}, leaf {i}"',
        )
        level1_tasks.append(t)

    # Wiring: node__0 -> all node__1_*
    start >> level1_tasks
