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

logger = logging.getLogger("airflow.test_dag")

args = {
    "owner": "airflow",
    "start_date": datetime(2024, 9, 1),
    "retries": 0,
}


@task
def task1():
    logger.info("Starting Task_1.")
    for i in range(50):
        logger.info("Task_1, iteration '%d'.", i)
        time.sleep(5)
    logger.info("Task_1 finished.")


@task
def task2():
    logger.info("Starting Task_2.")
    for i in range(3):
        logger.info("Task_2, iteration '%d'.", i)
    logger.info("Task_2 finished.")


with DAG(
    "test_dag",
    default_args=args,
    schedule=None,
    catchup=False,
) as dag:
    chain(task1(), task2())  # type: ignore
