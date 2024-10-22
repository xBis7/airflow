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
import contextlib
import pprint
import tempfile
import threading
from datetime import timedelta
from unittest import mock
from unittest.mock import MagicMock

import pytest
import subprocess
import pendulum
import time
import os

from testcontainers.redis import RedisContainer

from airflow.providers.celery.cli import celery_command

from airflow.api.common import trigger_dag
from airflow.executors import executor_loader

from airflow.models.serialized_dag import SerializedDagModel
from celery.backends.database import SessionManager, DatabaseBackend
from sqlalchemy.orm import sessionmaker, scoped_session

from airflow import settings
from sqlalchemy import create_engine, inspect, Table, MetaData, select

from airflow.utils.session import provide_session, create_session

from airflow.executors.sequential_executor import SequentialExecutor
from sqlalchemy.databases import postgres
from testcontainers.postgres import PostgresContainer

from airflow.utils.db import resetdb

from airflow.cli.commands.standalone_command import StandaloneCommand, standalone

from airflow.task.task_runner.standard_task_runner import StandardTaskRunner
from pygments.lexer import include

from airflow.callbacks.pipe_callback_sink import PipeCallbackSink
from airflow.cli import cli_parser
from airflow.cli.commands import task_command, db_command, dag_command

from airflow.executors.executor_utils import ExecutorName

from airflow.executors.executor_loader import ExecutorLoader

from airflow.executors.executor_constants import CELERY_EXECUTOR, LOCAL_EXECUTOR

from airflow.dag_processing.manager import DagFileProcessorAgent
from celery import Celery
from kombu.asynchronous import set_event_loop
from sqlalchemy_utils.types import uuid

from airflow.example_dags.example_external_task_marker_dag import start_date
from airflow.utils import timezone, db

from airflow.utils.types import DagRunType, DagRunTriggeredByType

from airflow.models.taskinstancekey import TaskInstanceKey

from airflow.models.taskinstance import SimpleTaskInstance, TaskInstance

from airflow.executors.base_executor import BaseExecutor

from airflow.jobs.job import Job

from airflow.jobs.scheduler_job_runner import SchedulerJobRunner

from airflow.configuration import conf
from airflow.providers.celery.executors import celery_executor, celery_executor_utils

from airflow.utils.state import TaskInstanceState, DagRunState, State

from airflow.traces.tracer import _Trace

from airflow.traces.otel_tracer import OtelTrace
from airflow.models import DagBag, DagRun, Base, DagModel

from opentelemetry import trace

from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

TRY_NUMBER = 1

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

    # postgres_airdb = PostgresContainer(
    #     image="postgres:latest",
    #     username="airflow",
    #     password="airflow",
    #     dbname="airflow"
    # )

    # Expose PostgreSQL to localhost
    # postgres.with_exposed_ports(5432)
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
def celery_worker(monkeypatch):
    os.environ["AIRFLOW__CORE__EXECUTOR"] = "CeleryExecutor"
    executor_name = ExecutorName(
        module_path="airflow.providers.celery.executors.celery_executor.CeleryExecutor",
        alias="CeleryExecutor"
    )
    monkeypatch.setattr(executor_loader, "_alias_to_executors", {"CeleryExecutor": executor_name})

    # Set up environment variables for the broker and result backend
    # broker_url = conf.get("celery", "broker_url")
    # result_backend = conf.get("celery", "result_backend")

    celery_command_args = [
        "celery",
        "--app", "airflow.providers.celery.executors.celery_executor.app",
        "worker",
        "--concurrency", "1",
        "--loglevel", "INFO",
    ]

    celery_worker_process = None
    try:
        celery_worker_process = subprocess.Popen(celery_command_args, env=os.environ.copy())
        # Wait a bit to ensure the worker starts
        time.sleep(5)
        yield executor_name, celery_worker_process
    finally:
        celery_worker_process.terminate()
        celery_worker_process.wait()

@pytest.fixture(scope="function")
def dag_bag():
    """Load DAGs from the same directory as the test script."""
    # Get the directory where the test script is located
    # test_dir = os.path.dirname(os.path.abspath(__file__))
    # dag_folder = os.path.join(test_dir, "dags")
    #
    # os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{dag_folder}"

    dag_folder = conf.get("core", "DAGS_FOLDER")

    # Load DAGs from that directory
    return DagBag(dag_folder=dag_folder, include_examples=False)

@pytest.fixture
def otel_tracer_with_in_memory_exporter(monkeypatch):
    """Set up InMemorySpanExporter to capture spans."""

    # Set up InMemorySpanExporter to capture spans locally
    memory_exporter = InMemorySpanExporter()

    # Create a tracer provider and batch span processor
    provider = TracerProvider()

    # Add exporter to the batch span processor
    memory_span_processor = BatchSpanProcessor(memory_exporter)

    provider.add_span_processor(memory_span_processor)

    # Set the tracer provider globally
    trace.set_tracer_provider(provider)

    otel_tracer = OtelTrace(span_exporter=memory_exporter)

    tracer_mock = MagicMock(wraps= lambda: otel_tracer)

    monkeypatch.setattr(_Trace, 'factory', tracer_mock)

    return memory_exporter, otel_tracer

def get_and_print_spans(otel_tracer, memory_exporter):
    # Force flush the spans
    otel_tracer.span_exporter.force_flush(timeout_millis=0)
    # Allow some time for spans to be sent
    time.sleep(10)
    # Retrieve the captured spans from the InMemorySpanExporter
    captured_spans = memory_exporter.get_finished_spans()

    # Assert that spans were captured by the InMemorySpanExporter
    assert len(captured_spans) > 0, "No spans were captured by the InMemorySpanExporter."

    # Print the captured spans for debugging
    for span in captured_spans:
        if span.parent is None:
            print(f"\nCaptured span: --START\n"
                  f"    trace_id: {span.get_span_context().trace_id},\n"
                  f"    span_id: {span.get_span_context().span_id},\n"
                  f"    parent: {span.parent},\n"
                  f"    span_name: {span.name},\n"
                  f"    Attributes: {span.attributes}\n"
                  f"-- END\n")
        else:
            print(f"\nCaptured span: --START\n"
                  f"    trace_id: {span.get_span_context().trace_id},\n"
                  f"    span_id: {span.get_span_context().span_id},\n"
                  f"    parent_trace_id: {span.parent.trace_id},\n"
                  f"    parent_span_id: {span.parent.span_id},\n"
                  f"    span_name: {span.name},\n"
                  f"    Attributes: {span.attributes}\n"
                  f"-- END\n")

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

def dump_celery_backend_db(celery_result_backend_uri):
    if celery_result_backend_uri is None:
        # celery_result_backend_uri = conf.get("celery", "result_backend")
        celery_result_backend_uri = conf.get("database", "sql_alchemy_conn")

    celery_engine = create_engine(celery_result_backend_uri)
    CelerySession = sessionmaker(bind=celery_engine)
    with CelerySession() as session:
        inspector = inspect(session.bind)
        all_tables = inspector.get_table_names()

        print("\n-----START_Celery-----\n")

        for table_name in all_tables:
            print(f"\nDumping table: {table_name}")
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=session.bind)
            query = session.query(table)
            results = [dict(row) for row in query.all()]
            # Pretty-print the table contents
            pprint.pprint({table_name: results}, width=120)

    print("\nCelery backend database dump complete.")
    print("\n-----END_Celery-----\n")


def test_sqlite_and_sequential_executor(monkeypatch, dag_bag, otel_tracer_with_in_memory_exporter):
    """Test generating spans using the OtelTrace class and retrieving them from the collector."""

    os.environ["AIRFLOW__CORE__EXECUTOR"] = "SequentialExecutor"
    executor_name = ExecutorName(module_path="airflow.executors.sequential_executor.SequentialExecutor", alias="SequentialExecutor")
    monkeypatch.setattr(executor_loader, "_alias_to_executors", {"SequentialExecutor": executor_name})

    parser = cli_parser.get_parser()

    db_command.initdb(cli_parser.get_parser().parse_args(["db", "init"]))
    # db.initdb()
    # resetdb()
    db_command.migratedb(parser.parse_args(["db", "migrate"]))

    # Create an instance of OtelTrace with InMemorySpanExporter for debugging
    global executor, value_tuple, dagrun
    memory_exporter, otel_tracer = otel_tracer_with_in_memory_exporter

    execution_date = pendulum.now("UTC")

    # Test: Trigger a DAG run
    dag_id = "test_dag"  # Replace with your actual DAG ID
    dag = dag_bag.get_dag(dag_id)

    # Ensure the DAG exists
    assert dag is not None, f"DAG with ID {dag_id} not found."

    with create_session() as sn:
        dump_airflow_metadata_db(sn)
        dag.sync_to_db(session=sn)

        # Manually serialize the dag to avoid waiting for the dag file processor manager to do it.
        SerializedDagModel.write_dag(dag, session=sn)
        sn.commit()

        async_mode = "sqlite" not in conf.get("database", "sql_alchemy_conn")
        processor_agent = DagFileProcessorAgent(
            dag_directory=dag_bag.dag_folder,
            max_runs=1,
            processor_timeout=timedelta(days=365),
            dag_ids=[],
            pickle_dags=False,
            async_mode=async_mode,
        )

        executor = SequentialExecutor()
        # executor.parallelism = 5
        executor.name = executor_name
        executor.callback_sink = PipeCallbackSink(get_sink_pipe=processor_agent.get_callbacks_pipe)

        data_interval = dag.timetable.infer_manual_data_interval(run_after=execution_date)
        dag_run = dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=execution_date,
            data_interval=data_interval,
            run_id=f"manual__{execution_date.isoformat()}",
            conf={},
            run_type=DagRunType.MANUAL,
            external_trigger=True,
            session=sn,
            triggered_by=DagRunTriggeredByType.TEST,
        )

        sn.add(dag_run)

        tis = dag_run.get_task_instances(session=sn)
        for ti in tis:
            ti.executor = str(executor.name)
            # Use SimpleTaskInstance which doesn't use db-bound attributes.
            executor.queue_command(ti,#SimpleTaskInstance.from_ti(ti),
                                   ["airflow", "tasks", "run", ti.dag_id, ti.task_id, "--local",
                                    str(dag_run.execution_date.isoformat())])

        sn.bulk_save_objects(tis)
        sn.commit()

        executor.start()

        job = Job(dag_id=dag_id, executor=executor)
        schedulerJobRunner = SchedulerJobRunner(
            job=job,
            subdir=dag_bag.dag_folder,
            num_runs=10,
            num_times_parse_dags=10
        )

        print(f"x: executor.queued_tasks: {executor.queued_tasks}")
        # do_scheduling
        # Run the scheduler loop.
        schedulerJobRunner._execute()

    # dag_dir = conf.get("core", "dags_folder")
    # print(f"x: dag_dir: {dag_dir}")
    #
    # executor = SequentialExecutor()
    # executor.name = ExecutorName(module_path="airflow.executors.sequential_executor.SequentialExecutor", alias="SequentialExecutor")
    #
    # tis = (task for task in dag.task_dict.values())
    # for ti in tis:
    #     ti.map_index = 1
    #     ti.try_number = 2
    #     ti.state = TaskInstanceState.SCHEDULED
    #
    #     print(f"name: {executor.name}")
    #     print(f"name: {executor.name.alias}")
    #     ti.executor = str(executor.name)
    #
    #     execution_date = now
    #     run_type = DagRunType.MANUAL
    #     data_interval = dag.timetable.infer_manual_data_interval(run_after=execution_date)
    #     triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
    #     dagrun = dag.create_dagrun(
    #         run_type=run_type,
    #         execution_date=execution_date,
    #         data_interval=data_interval,
    #         run_id=str(run_type.name + "_" + ti.task_id),
    #         start_date=now,
    #         state=DagRunState.QUEUED,
    #         external_trigger=False,
    #         **triggered_by_kwargs
    #     )
    #     ti.dag_run = dagrun
    #     ti.run_id = dagrun.run_id
    #
    #     task_instance_key = TaskInstanceKey(dag_id=ti.dag_id, task_id=ti.task_id, run_id=ti.run_id, try_number=ti.try_number, map_index=ti.map_index)
    #     # key = ("fail", "test_simple_ti", now, 1)
    #     # ti.key = key
    #     ti.key = task_instance_key
    #
    #     command = ["airflow", "tasks", "run", ti.dag_id, ti.task_id , "--local", str(dagrun.execution_date)]
    #     value_tuple = (
    #         command,
    #         1,
    #         None,
    #         ti
    #     )
    #
    #     executor.queued_tasks[task_instance_key] = value_tuple
    #
    # executor.start()
    #
    # # assert 0 == len(executor.queued_tasks), "Task should no longer be queued"
    # # assert executor.event_buffer[("fail", "fake_simple_ti", when, 0)][0] == State.FAILED
    #
    # job = Job(dag_id=dag_id, executor=executor)
    # schedulerJobRunner = SchedulerJobRunner(job=job, subdir=dag_bag.dag_folder, num_runs=3, num_times_parse_dags=3)
    #
    # # Run the scheduler loop.
    # schedulerJobRunner._execute()

    time.sleep(10)
    print("x: Out of the loop.")

    print("x: After.")

    tis = (task for task in dag.task_dict.values())
    for ti in tis:
        print(f"x: ti.dag_id: {ti.dag_id} | ti.task_id: {ti.task_id} | ti.state: {ti.state}")

    # TODO: get the dag run span context and store it in the dagrun as a field
    #   the task will get it from the dagrun and use it.
    #   Check when the dag run is marked as finished. When is the actual time.
    #   If the time is when the last task is marked as finished, then we can wrap the dagrun span
    #   around whatever call executes all the tasks.

    get_and_print_spans(otel_tracer, memory_exporter)

def test_postgres_and_celery_executor(monkeypatch, postgres_testcontainer, redis_testcontainer, celery_worker, dag_bag, otel_tracer_with_in_memory_exporter):
    """Test generating spans using the OtelTrace class and retrieving them from the collector."""

    executor_name, celery_worker_process = celery_worker


    psql_engine, psql_session = postgres_testcontainer

    parser = cli_parser.get_parser()

    # Create an instance of OtelTrace with ConsoleSpanExporter for debugging
    global executor, value_tuple, dagrun
    memory_exporter, otel_tracer = otel_tracer_with_in_memory_exporter

    execution_date = pendulum.now("UTC")

    with create_session() as sn:
        # Sync the dag_bag to db and make sure that we are reading the dags from the db.
        dag_bag.sync_to_db(session=sn)

        synced_dag_bag = DagBag(read_dags_from_db=True)

        dag_id = "test_dag"
        dag = synced_dag_bag.get_dag(dag_id)

        # Ensure the DAG exists
        assert dag is not None, f"DAG with ID {dag_id} not found."

        # Sync the dag to db.
        dag.sync_to_db(session=sn)
        sn.commit()

        # dump_airflow_metadata_db(sn)

        # print(f"x: sn.scalar(select(dag).where(dag.dag_id == dag_id)): {sn.scalar(select(DagModel).where(DagModel.dag_id == dag_id))}")

        # Manually serialize the dag to avoid waiting for the dag file processor manager to do it.
        SerializedDagModel.write_dag(dag, session=sn)
        sn.commit()

        async_mode = "sqlite" not in conf.get("database", "sql_alchemy_conn")
        processor_agent = DagFileProcessorAgent(
            dag_directory=dag_bag.dag_folder,
            max_runs=1,
            processor_timeout=timedelta(days=365),
            dag_ids=[],
            pickle_dags=False,
            async_mode=async_mode,
        )

        executor = celery_executor.CeleryExecutor()
        # executor.parallelism = 5
        executor.name = executor_name
        executor.callback_sink = PipeCallbackSink(get_sink_pipe=processor_agent.get_callbacks_pipe)

    # assert 0 == len(executor.queued_tasks), "Task should no longer be queued"
    # assert executor.event_buffer[("fail", "fake_simple_ti", when, 0)][0] == State.FAILED

        data_interval = dag.timetable.infer_manual_data_interval(run_after=execution_date)
        dag_run = dag.create_dagrun(
            state=DagRunState.QUEUED,
            execution_date=execution_date,
            data_interval=data_interval,
            run_id=f"manual__{execution_date.isoformat()}",
            conf={},
            run_type=DagRunType.MANUAL,
            external_trigger=True,
            session=sn,
            triggered_by=DagRunTriggeredByType.TEST,
        )

        sn.add(dag_run)

        # dump_airflow_metadata_db(sn)

        tis = dag_run.get_task_instances(session=sn)
        for ti in tis:
            ti.executor = str(executor.name)
            # Use SimpleTaskInstance which doesn't use db-bound attributes.
            # executor.queue_command(SimpleTaskInstance.from_ti(ti), ["airflow", "tasks", "run", ti.dag_id, ti.task_id , "--local", str(dag_run.execution_date.isoformat())])

        sn.bulk_save_objects(tis)
        sn.commit()

        # The scheduler starts the executor.
        # executor.start()

        job = Job(dag_id=dag_id, executor=executor)
        schedulerJobRunner = SchedulerJobRunner(
            job=job,
            subdir=dag_bag.dag_folder,
            num_runs=10,
        )

        # dump_airflow_metadata_db(sn)

        # trigger_dag.trigger_dag(
        #     dag_id=dag_id,
        #     triggered_by=DagRunTriggeredByType.TEST,
        #     run_id=dag_run.id,
        #     conf=conf,
        #     execution_date=execution_date,
        #     session=sn,
        # )

        # args = parser.parse_args(["celery", "worker", "--concurrency=1"])
        # celery_command.worker(args)

        print(f"x: executor.queued_tasks: {executor.queued_tasks}")
        print(f"x: executor.event_buffer: {executor.event_buffer}")
        # do_scheduling
        # Run the scheduler loop.
        schedulerJobRunner._execute()

        time.sleep(10)
        executor.sync()

        print(f"x: after - executor.queued_tasks: {executor.queued_tasks}")
        print(f"x: after - executor.event_buffer: {executor.event_buffer}")

        # dump_airflow_metadata_db(sn)
        # Tasks are queued. Execute them.

        # celery_executor_utils.execute_command()
        # executor.execute_async()

        # Run the scheduler loop again.


    print("x: Out of the loop.")

    # TODO: get the dag run span context and store it in the dagrun as a field
    #   the task will get it from the dagrun and use it.
    #   Check when the dag run is marked as finished. When is the actual time.
    #   If the time is when the last task is marked as finished, then we can wrap the dagrun span
    #   around whatever call executes all the tasks.

    with create_session() as session:
        # Convert execution_date to pendulum.DateTime if not already
        if not isinstance(execution_date, pendulum.DateTime):
            execution_date = pendulum.instance(execution_date)

        # Query TaskInstances for the given DAG ID and execution date
        task_instances = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.execution_date == execution_date
        ).all()

        if not task_instances:
            print(f"No tasks found for DAG '{dag_id}' at '{execution_date}'")

        for ti in task_instances:
            print(f"Task ID: {ti.task_id}, State: {ti.state}")

    get_and_print_spans(otel_tracer, memory_exporter)

    celery_worker_process.terminate()

def wait_for_task_completion(dag_id, max_wait_time=60):
    start_time = time.time()
    while time.time() - start_time < max_wait_time:
        with settings.Session() as session:
            task_instances = session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_id
            ).all()

            if not task_instances:
                time.sleep(5)
                continue

            all_done = all(
                ti.state in [State.SUCCESS, State.FAILED, State.SKIPPED]
                for ti in task_instances
            )
            if all_done:
                break
        time.sleep(5)

    # Print the final state of task instances
    for ti in task_instances:
        print(f"Task ID: {ti.task_id}, State: {ti.state}")

