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

import time
from datetime import datetime

from airflow import DAG
from airflow.sdk import Param, task

default_args = {
    "owner": "test",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

dag = DAG(
    "configurable_test_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_tasks=50,
    params={
        "num_tasks": Param(
            default=10, type="integer", minimum=1, maximum=5000, description="Number of test tasks to create"
        ),
        "sleep_duration": Param(
            default=5,
            type="integer",
            minimum=1,
            maximum=60,
            description="Sleep duration for each task in seconds",
        ),
    },
)


@task
def generate_task_numbers(**context):
    """Generate task numbers based on dag params"""
    num_tasks = context["params"]["num_tasks"]
    print(f"Generating {num_tasks} tasks")
    return list(range(num_tasks))


@task
def test_task(task_num: int, **context):
    """Configurable test task"""
    sleep_duration = context["params"]["sleep_duration"]
    print(f"task_{task_num}")
    time.sleep(sleep_duration)
    return f"Completed task_{task_num}"


@task
def summary_task(results: list, **context):
    """Summary of all completed tasks"""
    num_tasks = context["params"]["num_tasks"]
    print(f"Completed {len(results)}/{num_tasks} tasks")
    return {"requested": num_tasks, "completed": len(results)}


# Build the pipeline
task_numbers = generate_task_numbers()
mapped_tasks = test_task.expand(task_num=task_numbers)
summary = summary_task(mapped_tasks)

task_numbers >> mapped_tasks >> summary
