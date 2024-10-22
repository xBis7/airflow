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

# from airflow.configuration import conf


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

    data_folder = os.path.join(test_dir, "data")
    os.environ["AIRFLOW_HOME"] = f"{data_folder}"

    # os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:///:memory:"

    # os.environ["AIRFLOW__DATABASE__SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"

    # conf.remove_option("database", "sql_alchemy_conn")

    # print(f"x: conf.get(\"database\", \"sql_alchemy_conn\") {conf.get("database", "sql_alchemy_conn")}")

    # os.environ["AIRFLOW__CELERY__BROKER_URL"] = "memory://"

    # os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:////tmp/airflow_test.db"
    # os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:////:memory:"
    # os.environ["AIRFLOW__CELERY__RESULT_BACKEND"] = "db+sqlite:////tmp/airflow_test.db"
    # os.environ["AIRFLOW__CELERY__RESULT_BACKEND"] = str(db)

    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"


    # AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    # AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    # AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
    # AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0

