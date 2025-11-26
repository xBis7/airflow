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
Medium linear DAG: 50 tasks in a chain
Tests: Sequential scheduling at scale
"""

from __future__ import annotations

import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def simple_task(**context):
    time.sleep(0.1)
    return f"Done: {context['task_instance'].task_id}"


with DAG(
    dag_id="benchmark_02_linear_medium",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["benchmark", "linear", "medium"],
) as dag:
    # Generate 50 tasks programmatically
    tasks = []
    for i in range(1, 51):
        task = PythonOperator(
            task_id=f"task_{i:02d}",
            python_callable=simple_task,
        )
        tasks.append(task)

        # Link to previous task
        if i > 1:
            tasks[i - 2] >> task
