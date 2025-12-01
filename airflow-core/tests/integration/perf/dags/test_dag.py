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
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
}


# ---------------------------------------------------------------------------
# 1. Single DAG, 10 tasks in a straight line
# ---------------------------------------------------------------------------

with DAG(
    dag_id="bench_linear_10",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual trigger for clean benchmarks
    catchup=False,
    max_active_runs=50,  # tweak as needed for concurrency tests
    tags=["bench", "linear"],
) as bench_linear_10:
    prev = EmptyOperator(task_id="task_1")

    for i in range(2, 11):
        cur = EmptyOperator(task_id=f"task_{i}")
        prev >> cur
        prev = cur


# ---------------------------------------------------------------------------
# 2. One root task with 100 parallel downstream tasks
# ---------------------------------------------------------------------------

with DAG(
    dag_id="bench_fanout_1_100",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=20,
    tags=["bench", "fanout"],
) as bench_fanout_1_100:
    root = EmptyOperator(task_id="root")

    leaves = [EmptyOperator(task_id=f"leaf_{i}") for i in range(1, 101)]

    root >> leaves


# ---------------------------------------------------------------------------
# 4. Many small DAGs: 50 DAGs, each with 10 tasks in a line
#    DAG ids: bench_many_small_000 ... bench_many_small_049
# ---------------------------------------------------------------------------

NUM_DAGS = 50
TASKS_PER_DAG = 10

for d in range(NUM_DAGS):
    dag_id = f"bench_many_small_{d:03d}"

    with DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        max_active_runs=5,
        tags=["bench", "many-small"],
    ) as dag:
        prev = EmptyOperator(task_id="task_1")
        for i in range(2, TASKS_PER_DAG + 1):
            cur = EmptyOperator(task_id=f"task_{i}")
            prev >> cur
            prev = cur

    # register dynamically created DAG
    globals()[dag_id] = dag
