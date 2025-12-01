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
Linear DAG: 10 nodes in sequence

node__0 → node__1 → node__2 → node__3 → node__4 → node__5 → node__6 → node__7 → node__8 → node__9 → node__10
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
    dag_id="linear_dag",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    max_active_runs=5,
) as dag:
    # Create all nodes
    tasks = []
    for i in range(0, 11):
        t = BashOperator(
            task_id=f"node__{i}",
            bash_command=f'echo "Linear DAG -- Executing node__{i} (step {i})"',
        )
        tasks.append(t)

    # Wire in straight line
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]
