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

logger = logging.getLogger("airflow.duplicate_dispatch_dag")

args = {
    "owner": "airflow",
    "start_date": datetime(2024, 9, 1),
    # The incident's spurious failure was terminal because the task had no
    # retries; keep the same setting so a scheduler kill fails the dag run.
    "retries": 0,
}


@task
def long_running_task():
    logger.info("long_running_task started, sleeping.")
    for i in range(45):
        time.sleep(1)
        if i % 10 == 0:
            logger.info("long_running_task still sleeping (%ds).", i)
    logger.info("long_running_task finished.")


with DAG(
    "duplicate_dispatch_dag",
    default_args=args,
    schedule=None,
    catchup=False,
) as dag:
    long_running_task()
