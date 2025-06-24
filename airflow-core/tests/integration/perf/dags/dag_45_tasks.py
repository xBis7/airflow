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
from airflow.sdk import chain, task

logger = logging.getLogger("airflow.dag_45_tasks")

args = {
    "owner": "airflow",
    "start_date": datetime(2024, 9, 1),
    "retries": 0,
}


@task
def task1():
    logger.info("Starting task1.")
    time.sleep(10)


@task
def task2():
    logger.info("Starting task2.")
    time.sleep(10)


@task
def task3():
    logger.info("Starting task3.")
    time.sleep(10)


@task
def task4():
    logger.info("Starting task4.")
    time.sleep(10)


@task
def task5():
    logger.info("Starting task5.")
    time.sleep(10)


@task
def task6():
    logger.info("Starting task6.")
    time.sleep(10)


@task
def task7():
    logger.info("Starting task7.")
    time.sleep(10)


@task
def task8():
    logger.info("Starting task8.")
    time.sleep(10)


@task
def task9():
    logger.info("Starting task9.")
    time.sleep(10)


@task
def task10():
    logger.info("Starting task10.")
    time.sleep(10)


@task
def task11():
    logger.info("Starting task11.")
    time.sleep(10)


@task
def task12():
    logger.info("Starting task12.")
    time.sleep(10)


@task
def task13():
    logger.info("Starting task13.")
    time.sleep(10)


@task
def task14():
    logger.info("Starting task14.")
    time.sleep(10)


@task
def task15():
    logger.info("Starting task15.")
    time.sleep(10)


@task
def task16():
    logger.info("Starting task16.")
    time.sleep(10)


@task
def task17():
    logger.info("Starting task17.")
    time.sleep(10)


@task
def task18():
    logger.info("Starting task18.")
    time.sleep(10)


@task
def task19():
    logger.info("Starting task19.")
    time.sleep(10)


@task
def task20():
    logger.info("Starting task20.")
    time.sleep(10)


@task
def task21():
    logger.info("Starting task21.")
    time.sleep(10)


@task
def task22():
    logger.info("Starting task22.")
    time.sleep(10)


@task
def task23():
    logger.info("Starting task23.")
    time.sleep(10)


@task
def task24():
    logger.info("Starting task24.")
    time.sleep(10)


@task
def task25():
    logger.info("Starting task25.")
    time.sleep(10)


@task
def task26():
    logger.info("Starting task26.")
    time.sleep(10)


@task
def task27():
    logger.info("Starting task27.")
    time.sleep(10)


@task
def task28():
    logger.info("Starting task28.")
    time.sleep(10)


@task
def task29():
    logger.info("Starting task29.")
    time.sleep(10)


@task
def task30():
    logger.info("Starting task30.")
    time.sleep(10)


@task
def task31():
    logger.info("Starting task31.")
    time.sleep(10)


@task
def task32():
    logger.info("Starting task32.")
    time.sleep(10)


@task
def task33():
    logger.info("Starting task33.")
    time.sleep(10)


@task
def task34():
    logger.info("Starting task34.")
    time.sleep(10)


@task
def task35():
    logger.info("Starting task35.")
    time.sleep(10)


@task
def task36():
    logger.info("Starting task36.")
    time.sleep(10)


@task
def task37():
    logger.info("Starting task37.")
    time.sleep(10)


@task
def task38():
    logger.info("Starting task38.")
    time.sleep(10)


@task
def task39():
    logger.info("Starting task39.")
    time.sleep(10)


@task
def task40():
    logger.info("Starting task40.")
    time.sleep(10)


@task
def task41():
    logger.info("Starting task41.")
    time.sleep(10)


@task
def task42():
    logger.info("Starting task42.")
    time.sleep(10)


@task
def task43():
    logger.info("Starting task43.")
    time.sleep(10)


@task
def task44():
    logger.info("Starting task44.")
    time.sleep(10)


@task
def task45():
    logger.info("Starting task45.")
    time.sleep(10)


with DAG(
    "dag_45_tasks",
    default_args=args,
    schedule=None,
    catchup=False,
) as dag:
    chain(
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
        task11(),
        task12(),
        task13(),
        task14(),
        task15(),
        task16(),
        task17(),
        task18(),
        task19(),
        task20(),
        task21(),
        task22(),
        task23(),
        task24(),
        task25(),
        task26(),
        task27(),
        task28(),
        task29(),
        task30(),
        task31(),
        task32(),
        task33(),
        task34(),
        task35(),
        task36(),
        task37(),
        task38(),
        task39(),
        task40(),
        task41(),
        task42(),
        task43(),
        task44(),
        task45(),
    )  # type: ignore
