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

import os
from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

# The shape is fixed when the test serializes the Dag, before any process starts, so the sizes come
# from the environment rather than from a run's conf.
CHAIN_COUNT = int(os.environ.get("PERF_TEST__CHAIN_COUNT", "100"))
CHAIN_LENGTH = int(os.environ.get("PERF_TEST__CHAIN_LENGTH", "50"))

with DAG(
    dag_id="perf_chains_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0},
    tags=["perf"],
):
    # Every task instance waits on one upstream, while the run holds thousands of finished and
    # unfinished ones: the dependency checks of each pass search the finished ones once per waiting
    # task instance, and short chains keep the upstream walk of each check small.
    # EmptyOperator: the scheduler marks these succeeded itself, so no worker parses this file once
    # per task and the scheduler is the only constraint.
    for chain in range(1, CHAIN_COUNT + 1):
        previous = None
        for step in range(1, CHAIN_LENGTH + 1):
            current = EmptyOperator(task_id=f"chain_{chain}_step_{step}")
            if previous is not None:
                previous >> current
            previous = current
