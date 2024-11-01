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

import logging

import pytest
import subprocess
import pendulum
import time
import os

# from testcontainers.redis import RedisContainer

from airflow.executors import executor_loader

from airflow.models.serialized_dag import SerializedDagModel
from sqlalchemy.orm import sessionmaker, scoped_session

from airflow import settings
from sqlalchemy import create_engine, inspect

from airflow.traces.otel_tracer import CTX_PROP_SUFFIX
from airflow.utils.session import create_session

# from testcontainers.postgres import PostgresContainer

from airflow.utils.db import resetdb

from airflow.executors.executor_utils import ExecutorName

from airflow.configuration import conf
from airflow.providers.celery.executors import celery_executor

from airflow.utils.state import State

from airflow.models import DagBag, DagRun
from tests.otel.test_utils import (
    extract_spans_from_output,
    get_parent_child_dict,
    assert_span_name_belongs_to_root_span,
    assert_parent_children_spans,
    assert_span_not_in_children_spans,
    assert_parent_children_spans_for_non_root,
    dump_airflow_metadata_db
)

log = logging.getLogger("test_otel_integration")

# def redis_testcontainer():
#     # Start a Redis container for testing
#     redis = RedisContainer()
#
#     redis.with_bind_ports(6379, 6380)
#
#     redis.start()
#
#     # Get the connection URL to use in Airflow
#     redis_host = redis.get_container_host_ip()
#     redis_port = redis.get_exposed_port(6379)
#     broker_url = f"redis://{redis_host}:{redis_port}/0"
#
#     yield broker_url
#
# def postgres_testcontainer(monkeypatch, redis_testcontainer):
#     # Start a PostgreSQL container for testing
#     postgres = PostgresContainer(
#         image="postgres:latest",
#         username="airflow",
#         password="airflow",
#         dbname="airflow"
#     )
#
#     postgres.with_bind_ports(5432, 5433)
#
#     postgres.start()
#
#     try:
#         postgres_url = f"postgresql+psycopg2://airflow:airflow@localhost:{postgres.get_exposed_port(5432)}/airflow"
#         celery_backend_postgres_url = f"db+postgresql://airflow:airflow@localhost:{postgres.get_exposed_port(5432)}/airflow"
#
#         broker_url = redis_testcontainer
#
#         os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = postgres_url
#         os.environ["AIRFLOW__CELERY__BROKER_URL"] = broker_url
#         os.environ["AIRFLOW__CELERY__RESULT_BACKEND"] = celery_backend_postgres_url
#
#         psql_engine = create_engine(
#             postgres_url,
#             pool_size=5,  # Adjust pool size based on your needs
#             max_overflow=10,
#             pool_pre_ping=True,  # Ensures the connection is alive
#             pool_timeout=180,  # Increase if the connection is slow to establish
#             pool_recycle=1800
#         )
#         psql_session = scoped_session(sessionmaker(bind=psql_engine))
#
#         monkeypatch.setattr(settings, 'engine', psql_engine)
#         monkeypatch.setattr(settings, 'Session', psql_session)
#
#         # Update the Celery app's configuration to use the PostgreSQL backend
#         # celery_executor.app.conf.result_backend = celery_backend_postgres_url
#         celery_executor.app.conf.update(
#             result_backend=celery_backend_postgres_url,
#             broker_url=broker_url,
#         )
#
#         # Initialize the Airflow database
#         resetdb()
#
#         # Yield control back to the test
#         yield psql_engine, psql_session
#     finally:
#         postgres.stop()

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

def test_dag_spans_with_context_propagation(
    monkeypatch,
    dag_bag,
    celery_worker_args,
    capfd,
    session):
    """Test that a DAG runs successfully using CeleryExecutor and external scheduler and worker."""
    # Uncomment to enable debug mode and get span and db dumps on the output.

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
            log.info(f"DAG Run state: {dag_run_state}")

            if dag_run_state in [State.SUCCESS, State.FAILED]:
                break

        time.sleep(5)

    if logging.root.level == logging.DEBUG:
        with create_session() as session:
            dump_airflow_metadata_db(session)

    try:
        assert dag_run_state == State.SUCCESS, f"DAG run did not complete successfully. Final state: {dag_run_state}"
    finally:
        out, err = capfd.readouterr()
        log.info(f"out-start --\n{out}\n-- out-end")
        log.info(f"err-start --\n{err}\n-- err-end")

    output_lines = out.splitlines()

    root_span_dict, span_dict = extract_spans_from_output(output_lines)
    parent_child_dict = get_parent_child_dict(root_span_dict, span_dict)

    dag_span_name = str(dag_id + CTX_PROP_SUFFIX)
    assert_span_name_belongs_to_root_span(root_span_dict=root_span_dict, span_name=dag_span_name, should_succeed=True)

    non_existent_dag_span_name = str(dag_id + CTX_PROP_SUFFIX + "fail")
    assert_span_name_belongs_to_root_span(root_span_dict=root_span_dict, span_name=non_existent_dag_span_name, should_succeed=False)

    dag_children_span_names = []
    task_instance_ids = dag.task_ids

    for task_id in task_instance_ids:
        dag_children_span_names.append(f"{task_id}{CTX_PROP_SUFFIX}")

    first_task_id = task_instance_ids[0]

    assert_parent_children_spans(parent_child_dict=parent_child_dict, root_span_dict=root_span_dict,
                                 parent_name=dag_span_name, children_names=dag_children_span_names)

    assert_span_not_in_children_spans(parent_child_dict=parent_child_dict, root_span_dict=root_span_dict, span_dict=span_dict,
                                      parent_name=dag_span_name, child_name=first_task_id, span_exists=True)

    assert_span_not_in_children_spans(parent_child_dict=parent_child_dict, root_span_dict=root_span_dict, span_dict=span_dict,
                                      parent_name=dag_span_name, child_name=f"{first_task_id}_fail", span_exists=False)

    # Any spans generated under a task, are children of the task span.
    # The span hierarchy for dag 'test_dag' is
    # dag span
    #   |_ task_1 span
    #       |_ sub_span_1
    #           |_ sub_span_2
    #               |_ sub_span_3
    #       |_ sub_span_4
    #   |_ task_2 span

    first_task_children_span_names = [
        f"{first_task_id}_sub_span1{CTX_PROP_SUFFIX}",
        f"{first_task_id}_sub_span4{CTX_PROP_SUFFIX}"
    ]
    assert_parent_children_spans_for_non_root(span_dict=span_dict, parent_name=f"{first_task_id}{CTX_PROP_SUFFIX}", children_names=first_task_children_span_names)

    # Single element list.
    sub_span1_children_span_names = [
        f"{first_task_id}_sub_span2{CTX_PROP_SUFFIX}"
    ]
    assert_parent_children_spans_for_non_root(span_dict=span_dict, parent_name=f"{first_task_id}_sub_span1{CTX_PROP_SUFFIX}", children_names=sub_span1_children_span_names)

    sub_span2_children_span_names = [
        f"{first_task_id}_sub_span3{CTX_PROP_SUFFIX}"
    ]
    assert_parent_children_spans_for_non_root(span_dict=span_dict, parent_name=f"{first_task_id}_sub_span2{CTX_PROP_SUFFIX}", children_names=sub_span2_children_span_names)
