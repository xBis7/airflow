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
import threading

import pytest
import subprocess
import pendulum
import time
import os

from airflow import settings
from airflow.models.dag import (
    DAG,
    DAG_ARGS_EXPECTED_TYPES,
    DagModel,
    DagOwnerAttributes,
    DagTag,
    ExecutorLoader,
    dag as dag_decorator,
    get_dataset_triggered_next_run_info,
)
from airflow.traces.otel_tracer import OtelTrace
from airflow.utils.types import DagRunTriggeredByType
from airflow.models import DagBag
from airflow.utils.state import State, DagRunState
from airflow.cli import cli_parser
from airflow.cli.commands import dag_command

from opentelemetry import trace

from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from tests.test_utils.compat import AIRFLOW_V_3_0_PLUS

@pytest.fixture(scope="session")
def airflow_db():
    """Fixture to initialize the Airflow SQLite database."""
    # Path to the SQLite database
    sqlite_db_path = "/tmp/airflow.db"

    # Manually set the environment variable for Airflow's database connection string
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{sqlite_db_path}"

    # Run Airflow database migration command to initialize the db
    subprocess.run(["airflow", "db", "migrate"], check=True)

    # Ensure the database file is created
    assert os.path.exists(sqlite_db_path)

    # Yield the database path for use in tests
    yield sqlite_db_path

@pytest.fixture(scope="function")
def dag_bag():
    """Load DAGs from the same directory as the test script."""
    # Get the directory where the test script is located
    test_dir = os.path.dirname(os.path.abspath(__file__))
    dag_folder = os.path.join(test_dir, "dags")

    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{dag_folder}"

    # Load DAGs from that directory
    return DagBag(dag_folder=dag_folder)

@pytest.fixture
def get_in_memory_span_exporter():
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

    return memory_exporter

def test_otel_trace_integration(airflow_db, dag_bag, get_in_memory_span_exporter):
    """Test generating spans using the OtelTrace class and retrieving them from the collector."""
    memory_exporter = get_in_memory_span_exporter

    # Create an instance of OtelTrace with ConsoleSpanExporter for debugging
    otel_tracer = OtelTrace(span_exporter=memory_exporter)

    now = pendulum.now("UTC")

    # with otel_tracer.start_span("test_span") as span:
    #     span.set_attribute("test_attribute", "test_value")
    #     print(f"Generated span with ID: {span.get_span_context().span_id}")

    # Test: Verify that the SQLite DB is initialized
    assert os.path.exists(airflow_db)

    # Test: Trigger a DAG run
    dag_id = "test_dag"  # Replace with your actual DAG ID
    dag = dag_bag.get_dag(dag_id)

    # Ensure the DAG exists
    assert dag is not None, f"DAG with ID {dag_id} not found."

    session = settings.Session()
    orm_dag = DagModel(
        dag_id=dag.dag_id,
        max_active_tasks=1,
        has_task_concurrency_limits=False,
        next_dagrun=dag.start_date,
        next_dagrun_create_after=now,
        is_active=True,
    )
    session.add(orm_dag)
    session.flush()

    # query, _ = DagModel.dags_needing_dagruns(session)
    # dag_models = query.all()
    # assert dag_models == []
    #
    # session.rollback()
    # session.close()

    unpause_args = cli_parser.get_parser().parse_args(["dags", "unpause", "test_dag"])
    dag_command.dag_unpause(unpause_args)

    time.sleep(5)

    trigger_args = cli_parser.get_parser().parse_args(["dags", "trigger", "test_dag"])
    dag_command.dag_trigger(trigger_args)

    time.sleep(5)

    # Trigger a DAG run

    # triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.CLI} if AIRFLOW_V_3_0_PLUS else {}
    # dagrun = dag.create_dagrun(
    #     run_id="test_dagrun",
    #     state=DagRunState.RUNNING,
    #     execution_date=now,
    #     data_interval=dag.timetable.infer_manual_data_interval(run_after=now),
    #     start_date=now,
    #     **triggered_by_kwargs,
    # )
    #
    # dagrun.queued_at = now

    # Assert that the DAG run was created
    # assert dagrun is not None
    # assert dagrun.state == State.RUNNING
    #
    # with otel_tracer.start_span_from_dagrun(dagrun) as span:
    #     span.set_attribute("test_attribute", "test_value")
    #     print(f"Generated span: {span.get_span_context().span_id}")
    #
    # time.sleep(10)

    # Force flush the spans
    otel_tracer.span_exporter.force_flush(timeout_millis=0)

    # Allow some time for spans to be sent
    time.sleep(3)

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

    # Example: Check that the span has the correct attributes
    # assert captured_spans[0].name == "test_span", "Captured span name does not match."
    # assert captured_spans[0].attributes[
    #            "test_attribute"] == "test_value", "Captured span attribute does not match."
