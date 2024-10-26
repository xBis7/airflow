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

import pprint

import pytest
import subprocess
import pendulum
import time
import os

from testcontainers.redis import RedisContainer

from airflow.executors import executor_loader

from airflow.models.serialized_dag import SerializedDagModel
from sqlalchemy.orm import sessionmaker, scoped_session

from airflow import settings
from sqlalchemy import create_engine, inspect

from airflow.utils.session import create_session

from testcontainers.postgres import PostgresContainer

from airflow.utils.db import resetdb

from airflow.executors.executor_utils import ExecutorName

from airflow.configuration import conf
from airflow.providers.celery.executors import celery_executor

from airflow.utils.state import State

from airflow.models import DagBag, DagRun, Base

@pytest.fixture
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

@pytest.fixture
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

@pytest.fixture
def airflow_scheduler(celery_worker, postgres_testcontainer, redis_testcontainer):
    scheduler_command_args = [
        "airflow",
        "scheduler",
    ]

    scheduler_process = None
    try:
        scheduler_process = subprocess.Popen(
            scheduler_command_args,
            env=os.environ.copy(),
            # stdout=subprocess.PIPE,
            # stderr=subprocess.PIPE,
            stdout=None,
            stderr=None,
        )
        # Wait a bit to ensure the scheduler starts
        time.sleep(10)
        yield scheduler_process
    finally:
        if scheduler_process:
            scheduler_process.terminate()
            scheduler_process.wait()

@pytest.fixture
def celery_worker(monkeypatch):
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

    celery_worker_process = None
    try:
        celery_worker_process = subprocess.Popen(
            celery_command_args,
            env=os.environ.copy(),
            stdout=None,
            stderr=None,
        )
        # Wait a bit to ensure the worker starts
        time.sleep(5)
        yield executor_name, celery_worker_process
    finally:
        celery_worker_process.terminate()
        celery_worker_process.wait()

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

def test_postgres_and_celery_executor(
    monkeypatch,
    postgres_testcontainer,
    airflow_scheduler,
    celery_worker,
    dag_bag,
):
    """Test that a DAG runs successfully using CeleryExecutor and external scheduler and worker."""
    psql_engine, psql_session = postgres_testcontainer

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
    max_wait_time = 30  # seconds
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

    with create_session() as session:
        dump_airflow_metadata_db(session)

    assert dag_run_state == State.SUCCESS, f"DAG run did not complete successfully. Final state: {dag_run_state}"
