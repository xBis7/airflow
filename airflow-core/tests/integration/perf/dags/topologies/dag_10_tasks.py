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

import logging
import time
from datetime import datetime

from airflow import DAG
from airflow.sdk import task

logger = logging.getLogger("airflow.dag_10_tasks")

args = {
    "owner": "airflow",
    "start_date": datetime(2024, 9, 1),
    "retries": 0,
}


@task
def task1():
    logger.info("Starting task1.")
    time.sleep(3)


@task
def task2():
    logger.info("Starting task2.")
    time.sleep(3)


@task
def task3():
    logger.info("Starting task3.")
    time.sleep(3)


@task
def task4():
    logger.info("Starting task4.")
    time.sleep(3)


@task
def task5():
    logger.info("Starting task5.")
    time.sleep(3)


@task
def task6():
    logger.info("Starting task6.")
    time.sleep(3)


@task
def task7():
    logger.info("Starting task7.")
    time.sleep(3)


@task
def task8():
    logger.info("Starting task8.")
    time.sleep(3)


@task
def task9():
    logger.info("Starting task9.")
    time.sleep(3)


@task
def task10():
    logger.info("Starting task10.")
    time.sleep(3)


with DAG(
    "dag_10_tasks",
    default_args=args,
    schedule=None,
    catchup=False,
) as dag:
    parallel_tasks = [
        task1(),
        task2(),
        task3(),
        task4(),
        task5(),
        task6(),
        task7(),
        task8(),
        task9(),
        task10(),
    ]
