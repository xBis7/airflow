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
Binary tree DAG: Each task spawns 2 children
Depth 4 = 31 total tasks
Tests: Gradual parallelism increase
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
    dag_id="benchmark_05_binary_tree",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["benchmark", "tree", "branching"],
) as dag:
    # Level 0 (root)
    t0 = PythonOperator(task_id="task_00", python_callable=simple_task)

    # Level 1
    t1 = PythonOperator(task_id="task_01", python_callable=simple_task)
    t2 = PythonOperator(task_id="task_02", python_callable=simple_task)
    t0 >> [t1, t2]

    # Level 2
    t3 = PythonOperator(task_id="task_03", python_callable=simple_task)
    t4 = PythonOperator(task_id="task_04", python_callable=simple_task)
    t5 = PythonOperator(task_id="task_05", python_callable=simple_task)
    t6 = PythonOperator(task_id="task_06", python_callable=simple_task)
    t1 >> [t3, t4]
    t2 >> [t5, t6]

    # Level 3
    t7 = PythonOperator(task_id="task_07", python_callable=simple_task)
    t8 = PythonOperator(task_id="task_08", python_callable=simple_task)
    t9 = PythonOperator(task_id="task_09", python_callable=simple_task)
    t10 = PythonOperator(task_id="task_10", python_callable=simple_task)
    t11 = PythonOperator(task_id="task_11", python_callable=simple_task)
    t12 = PythonOperator(task_id="task_12", python_callable=simple_task)
    t13 = PythonOperator(task_id="task_13", python_callable=simple_task)
    t14 = PythonOperator(task_id="task_14", python_callable=simple_task)
    t3 >> [t7, t8]
    t4 >> [t9, t10]
    t5 >> [t11, t12]
    t6 >> [t13, t14]

    # Level 4 (leaves)
    t15 = PythonOperator(task_id="task_15", python_callable=simple_task)
    t16 = PythonOperator(task_id="task_16", python_callable=simple_task)
    t17 = PythonOperator(task_id="task_17", python_callable=simple_task)
    t18 = PythonOperator(task_id="task_18", python_callable=simple_task)
    t19 = PythonOperator(task_id="task_19", python_callable=simple_task)
    t20 = PythonOperator(task_id="task_20", python_callable=simple_task)
    t21 = PythonOperator(task_id="task_21", python_callable=simple_task)
    t22 = PythonOperator(task_id="task_22", python_callable=simple_task)
    t23 = PythonOperator(task_id="task_23", python_callable=simple_task)
    t24 = PythonOperator(task_id="task_24", python_callable=simple_task)
    t25 = PythonOperator(task_id="task_25", python_callable=simple_task)
    t26 = PythonOperator(task_id="task_26", python_callable=simple_task)
    t27 = PythonOperator(task_id="task_27", python_callable=simple_task)
    t28 = PythonOperator(task_id="task_28", python_callable=simple_task)
    t29 = PythonOperator(task_id="task_29", python_callable=simple_task)
    t30 = PythonOperator(task_id="task_30", python_callable=simple_task)

    t7 >> [t15, t16]
    t8 >> [t17, t18]
    t9 >> [t19, t20]
    t10 >> [t21, t22]
    t11 >> [t23, t24]
    t12 >> [t25, t26]
    t13 >> [t27, t28]
    t14 >> [t29, t30]
