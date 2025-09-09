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

# Default arguments for the DAG
default_args = {
    "owner": "test",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

# Create the DAG
dag = DAG(
    "dag_470_tasks",
    default_args=default_args,
    description="Test DAG with 470 tasks for scheduler testing",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["test", "scheduler"],
)

# Create 470 tasks
for i in range(1, 471):
    # Determine priority: every 10th task gets higher priority
    if i % 10 == 0:
        priority = 100  # High priority for every 10th task
        task_name = f"high_priority_task_{i}"
        message = f"HIGH PRIORITY TASK {i}: This is task number {i} with elevated priority"
    else:
        priority = 1  # Normal priority for other tasks
        task_name = f"normal_task_{i}"
        message = f"NORMAL TASK {i}: This is task number {i} with normal priority"

    # Create task using BashOperator
    task = BashOperator(
        task_id=task_name,
        bash_command=f'echo "{message}"',
        priority_weight=priority,
        pool_slots=1,
        dag=dag,
    )
