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

import pytest
import os

@pytest.fixture(scope='session', autouse=True)
def config_setup():
    print(f"x: config_setup fixture")
    os.environ["AIRFLOW__TRACES__OTEL_ON"] = "True"
    os.environ["AIRFLOW__TRACES__OTEL_HOST"] = "localhost"
    os.environ["AIRFLOW__TRACES__OTEL_PORT"] = "4318"
    os.environ["AIRFLOW__TRACES__OTEL_DEBUGGING_ON"] = "True"
    os.environ["AIRFLOW__TRACES__OTEL_TASK_LOG_EVENT"] = "True"
    os.environ["AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR"] = "False"
    os.environ["AIRFLOW__SCHEDULER__PROCESSOR_POLL_INTERVAL"] = "2"

    test_dir = os.path.dirname(os.path.abspath(__file__))
    dag_folder = os.path.join(test_dir, "dags")
    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{dag_folder}"

    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"
