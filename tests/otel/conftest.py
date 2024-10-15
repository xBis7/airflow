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

from airflow.configuration import conf

@pytest.fixture(scope='session', autouse=True)
def config_setup():
    print(f"x: config_setup fixture")
    os.environ["AIRFLOW__TRACES__OTEL_ON"] = "True"
    os.environ["AIRFLOW__TRACES__OTEL_HOST"] = "localhost"
    os.environ["AIRFLOW__TRACES__OTEL_PORT"] = "4318"
    os.environ["AIRFLOW__TRACES__OTEL_DEBUGGING_ON"] = "False"
    os.environ["AIRFLOW__TRACES__OTEL_TASK_LOG_EVENT"] = "True"
    os.environ["AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR"] = "False"
    os.environ["AIRFLOW__SCHEDULER__PROCESSOR_POLL_INTERVAL"] = "2"

    os.environ["AIRFLOW__CELERY__BROKER_URL"] = "memory://"
    os.environ["AIRFLOW__CELERY__RESULT_BACKEND"] = "db+sqlite:///memory"

    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"

    # conf.__init__()
    # conf.add_section("traces")
    # conf.set(section="traces", option="otel_on", value="True")
    # conf.set(section="traces", option="otel_host", value="localhost")
    # conf.set(section="traces", option="otel_port", value="4318")
    # conf.set(section="traces", option="otel_debugging_on", value="False")
    # conf.set(section="traces", option="otel_task_log_event", value="True")

