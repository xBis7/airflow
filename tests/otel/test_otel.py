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
import io
import json
import pprint
from contextlib import redirect_stdout

import pytest
import subprocess
import pendulum
import time
import os

from airflow.traces import otel_tracer
from testcontainers.redis import RedisContainer

from airflow.executors import executor_loader

from airflow.models.serialized_dag import SerializedDagModel
from sqlalchemy.orm import sessionmaker, scoped_session

from airflow import settings
from sqlalchemy import create_engine, inspect

from airflow.traces.tracer import Trace
from airflow.utils.session import create_session

from testcontainers.postgres import PostgresContainer

from airflow.utils.db import resetdb

from airflow.executors.executor_utils import ExecutorName

from airflow.configuration import conf
from airflow.providers.celery.executors import celery_executor

from airflow.utils.state import State

from airflow.models import DagBag, DagRun, Base

@pytest.fixture(scope='function')
def redis_testcontainer():
    # Start a Redis container for testing
    redis = RedisContainer()

    redis.with_bind_ports(6379, 6380)

    redis.start()

    # Get the connection URL to use in Airflow
    redis_host = redis.get_container_host_ip()
    redis_port = redis.get_exposed_port(6379)
    broker_url = f"redis://{redis_host}:{redis_port}/0"

    yield broker_url

@pytest.fixture(scope='function')
def postgres_testcontainer(monkeypatch, redis_testcontainer):
    # Start a PostgreSQL container for testing
    postgres = PostgresContainer(
        image="postgres:latest",
        username="airflow",
        password="airflow",
        dbname="airflow"
    )

    postgres.with_bind_ports(5432, 5433)

    postgres.start()

    try:

        print(f"x: postgres.get_exposed_port(5432): {postgres.get_exposed_port(5432)}")
        print(f"x: postgres.get_container_host_ip(): {postgres.get_container_host_ip()}")
        print(f"x: postgres.get_connection_url(): {postgres.get_connection_url()}")

        postgres_url = f"postgresql+psycopg2://airflow:airflow@localhost:{postgres.get_exposed_port(5432)}/airflow"
        celery_backend_postgres_url = f"db+postgresql://airflow:airflow@localhost:{postgres.get_exposed_port(5432)}/airflow"

        broker_url = redis_testcontainer

        os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = postgres_url
        os.environ["AIRFLOW__CELERY__BROKER_URL"] = broker_url
        os.environ["AIRFLOW__CELERY__RESULT_BACKEND"] = celery_backend_postgres_url

        psql_engine = create_engine(
            postgres_url,
            pool_size=5,  # Adjust pool size based on your needs
            max_overflow=10,
            pool_pre_ping=True,  # Ensures the connection is alive
            pool_timeout=180,  # Increase if the connection is slow to establish
            pool_recycle=1800
        )
        psql_session = scoped_session(sessionmaker(bind=psql_engine))

        monkeypatch.setattr(settings, 'engine', psql_engine)
        monkeypatch.setattr(settings, 'Session', psql_session)

        # Update the Celery app's configuration to use the PostgreSQL backend
        # celery_executor.app.conf.result_backend = celery_backend_postgres_url
        celery_executor.app.conf.update(
            result_backend=celery_backend_postgres_url,
            broker_url=broker_url,
        )

        # Initialize the Airflow database
        resetdb()

        # Yield control back to the test
        yield psql_engine, psql_session
    finally:
        postgres.stop()

@pytest.fixture(scope='function')
def airflow_scheduler_args(capfd):
    scheduler_command_args = [
        "airflow",
        "scheduler",
    ]

    yield scheduler_command_args

@pytest.fixture(scope='function')
def celery_worker_args(monkeypatch):
    os.environ["AIRFLOW__CORE__EXECUTOR"] = "CeleryExecutor"
    executor_name = ExecutorName(
        module_path="airflow.providers.celery.executors.celery_executor.CeleryExecutor",
        alias="CeleryExecutor"
    )
    monkeypatch.setattr(executor_loader, "_alias_to_executors", {"CeleryExecutor": executor_name})

    celery_command_args = [
        "celery",
        "--app", "airflow.providers.celery.executors.celery_executor.app",
        "worker",
        "--concurrency", "1",
        "--loglevel", "INFO",
    ]

    yield celery_command_args

@pytest.fixture(scope="function")
def dag_bag():
    """Load DAGs from the same directory as the test script."""
    dag_folder = conf.get("core", "DAGS_FOLDER")

    # Load DAGs from that directory
    return DagBag(dag_folder=dag_folder, include_examples=False)

def dump_airflow_metadata_db(session):
    inspector = inspect(session.bind)
    all_tables = inspector.get_table_names()

    # dump with the entire db
    db_dump = {}

    print("\n-----START_airDb-----\n")

    for table_name in all_tables:
        print(f"\nDumping table: {table_name}")
        table = Base.metadata.tables.get(table_name)
        if table is not None:
            query = session.query(table)
            results = [dict(row) for row in query.all()]
            db_dump[table_name] = results
            # Pretty-print the table contents
            if table_name == "connection":
                filtered_results = [row for row in results if row.get('conn_id') == 'airflow_db']
                pprint.pprint({table_name: filtered_results}, width=120)
            else:
                pprint.pprint({table_name: results}, width=120)
        else:
            print(f"Table {table_name} not found in metadata.")

    print("\nAirflow metadata database dump complete.")
    print("\n-----END_airDb-----\n")

def extract_spans_from_output(output_lines):
    spans = []
    total_lines = len(output_lines)
    index = 0

    while index < total_lines:
        line = output_lines[index].strip()
        if line.startswith('{') and line == '{':
            # Start of a JSON object
            json_lines = [line]
            index += 1
            while index < total_lines:
                line = output_lines[index]
                json_lines.append(line)
                if line.strip().startswith('}') and line == '}':
                    # End of the JSON object
                    break
                index += 1
            # Attempt to parse the collected lines as JSON
            json_str = '\n'.join(json_lines)
            try:
                span = json.loads(json_str)
                spans.append(span)
            except json.JSONDecodeError as e:
                # Handle or log the error if needed
                print(f"Failed to parse JSON span: {e}")
                print("Failed JSON string:")
                print(json_str)
        else:
            index += 1

    return spans

def test_postgres_and_celery_executor(
    monkeypatch,
    postgres_testcontainer,
    airflow_scheduler_args,
    celery_worker_args,
    dag_bag,
    capfd
):
    """Test that a DAG runs successfully using CeleryExecutor and external scheduler and worker."""
    psql_engine, psql_session = postgres_testcontainer

    celery_command_args = celery_worker_args

    celery_worker_process = subprocess.Popen(
        celery_command_args,
        env=os.environ.copy(),
        stdout=None,
        stderr=None,
    )

    # Start the processes here and not as fixtures, so that the test can capture their output.
    scheduler_command_args = airflow_scheduler_args

    scheduler_process = subprocess.Popen(
        scheduler_command_args,
        env=os.environ.copy(),
        stdout=None,
        stderr=None,
    )

    # Wait a bit to ensure both processes have started
    time.sleep(10)

    execution_date = pendulum.now("UTC")

    # Ensure the DAG is loaded
    dag_id = "test_dag"
    dag = dag_bag.get_dag(dag_id)

    assert dag is not None, f"DAG with ID {dag_id} not found."

    with create_session() as session:
        # Sync the DAG to the database
        dag.sync_to_db(session=session)
        SerializedDagModel.write_dag(dag, session=session)
        session.commit()

    unpause_command = [
        "airflow",
        "dags",
        "unpause",
        dag_id
    ]

    subprocess.run(unpause_command, check=True, env=os.environ.copy())

    run_id = f"manual__{execution_date.isoformat()}"

    # Trigger the DAG run using the Airflow CLI
    trigger_command = [
        "airflow",
        "dags",
        "trigger",
        dag_id,
        "--run-id", run_id,
        "--exec-date", execution_date.isoformat(),
    ]

    subprocess.run(trigger_command, check=True, env=os.environ.copy())

    # Wait until the DAG run completes
    max_wait_time = 60  # seconds
    start_time = time.time()

    dag_run_state = None

    while time.time() - start_time < max_wait_time:
        with create_session() as session:
            dag_run = session.query(DagRun).filter(
                DagRun.dag_id == dag_id,
                DagRun.run_id == run_id,
            ).first()

            if dag_run is None:
                time.sleep(5)
                continue

            dag_run_state = dag_run.state
            print(f"DAG Run state: {dag_run_state}")

            if dag_run_state in [State.SUCCESS, State.FAILED]:
                break

        time.sleep(5)

    # with create_session() as session:
    #     dump_airflow_metadata_db(session)

    assert dag_run_state == State.SUCCESS, f"DAG run did not complete successfully. Final state: {dag_run_state}"

    # Terminate the processes.
    celery_worker_process.terminate()
    celery_worker_process.wait()

    scheduler_process.terminate()
    scheduler_process.wait()

    out, err = capfd.readouterr()

    output_lines = out.splitlines()

    # Example span from the ConsoleSpanExporter
    #     {
    #         "name": "perform_heartbeat",
    #         "context": {
    #             "trace_id": "0xa18781ea597c3d07c85e95fd3a6d7d40",
    #             "span_id": "0x8ae7bb13ec5b28ba",
    #             "trace_state": "[]"
    #         },
    #         "kind": "SpanKind.INTERNAL",
    #         "parent_id": "0x17ac77a4a840758d",
    #         "start_time": "2024-10-30T16:19:33.947155Z",
    #         "end_time": "2024-10-30T16:19:33.947192Z",
    #         "status": {
    #             "status_code": "UNSET"
    #         },
    #         "attributes": {},
    #         "events": [],
    #         "links": [],
    #         "resource": {
    #             "attributes": {
    #                 "telemetry.sdk.language": "python",
    #                 "telemetry.sdk.name": "opentelemetry",
    #                 "telemetry.sdk.version": "1.27.0",
    #                 "host.name": "host.local",
    #                 "service.name": "Airflow"
    #             },
    #             "schema_url": ""
    #         }
    #     }
    spans = extract_spans_from_output(output_lines)
    # print(f"x: spans-start: {spans} -- spans-end")

    for span in spans:
        print(f"span:   | name: {span['name']} | spanid: {span['context']['span_id']} | traceid: {span['context']['trace_id']}")
        # if span.parent is not None:
        #     print(f"        | parent.spanid: {span.parent.span_id} | parent.traceid: {span.parent.trace_id}")
        # else:
        #     print(f"        | span is root and has no parent.")
