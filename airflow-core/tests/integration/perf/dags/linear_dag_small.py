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
Simple linear DAG: 10 tasks in a chain
Tests: Basic sequential scheduling
"""

from __future__ import annotations

import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def simple_task(**context):
    """Just sleep for 0.1 seconds"""
    time.sleep(0.1)
    return f"Done: {context['task_instance'].task_id}"


with DAG(
    dag_id="benchmark_01_linear_small",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["benchmark", "linear", "small"],
) as dag:
    # Create 10 tasks in a chain
    t1 = PythonOperator(task_id="task_01", python_callable=simple_task)
    t2 = PythonOperator(task_id="task_02", python_callable=simple_task)
    t3 = PythonOperator(task_id="task_03", python_callable=simple_task)
    t4 = PythonOperator(task_id="task_04", python_callable=simple_task)
    t5 = PythonOperator(task_id="task_05", python_callable=simple_task)
    t6 = PythonOperator(task_id="task_06", python_callable=simple_task)
    t7 = PythonOperator(task_id="task_07", python_callable=simple_task)
    t8 = PythonOperator(task_id="task_08", python_callable=simple_task)
    t9 = PythonOperator(task_id="task_09", python_callable=simple_task)
    t10 = PythonOperator(task_id="task_10", python_callable=simple_task)

    # Set up the chain
    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10
