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
from datetime import timedelta
from unittest import mock
from unittest.mock import MagicMock

import pytest
import subprocess
import pendulum
import time
import os

from pygments.lexer import include

from airflow.callbacks.pipe_callback_sink import PipeCallbackSink

from airflow.executors.executor_utils import ExecutorName

from airflow.executors.executor_loader import ExecutorLoader

from airflow.executors.executor_constants import CELERY_EXECUTOR, LOCAL_EXECUTOR

from airflow.dag_processing.manager import DagFileProcessorAgent
from celery import Celery
from kombu.asynchronous import set_event_loop
from sqlalchemy_utils.types import uuid

from airflow.example_dags.example_external_task_marker_dag import start_date

from airflow.utils.types import DagRunType, DagRunTriggeredByType

from airflow.models.taskinstancekey import TaskInstanceKey

from airflow.models.taskinstance import SimpleTaskInstance, TaskInstance

from airflow.cli.cli_parser import executor

from airflow.executors.base_executor import BaseExecutor

from airflow.jobs.job import Job

from airflow.jobs.scheduler_job_runner import SchedulerJobRunner

from airflow.configuration import conf
from airflow.providers.celery.executors import celery_executor

from airflow.utils.state import TaskInstanceState

from airflow.traces.tracer import _Trace

from airflow.traces.otel_tracer import OtelTrace
from airflow.models import DagBag, DagRun

from opentelemetry import trace

from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from tests.listeners.partial_listener import state
from tests.test_utils.compat import AIRFLOW_V_3_0_PLUS

@pytest.fixture(scope="function")
def dag_bag():
    """Load DAGs from the same directory as the test script."""
    # Get the directory where the test script is located
    test_dir = os.path.dirname(os.path.abspath(__file__))
    dag_folder = os.path.join(test_dir, "dags")

    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{dag_folder}"

    # Load DAGs from that directory
    return DagBag(dag_folder=dag_folder, include_examples=False)

@pytest.fixture
def setup_otel_tracer_with_in_memory_span_exporter(monkeypatch):
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

def test_otel_trace_integration(monkeypatch, dag_bag, setup_otel_tracer_with_in_memory_span_exporter):
    """Test generating spans using the OtelTrace class and retrieving them from the collector."""
    # Create an instance of OtelTrace with ConsoleSpanExporter for debugging
    global executor
    memory_exporter, otel_tracer = setup_otel_tracer_with_in_memory_span_exporter

    now = pendulum.now("UTC")

    # Test: Trigger a DAG run
    dag_id = "test_dag"  # Replace with your actual DAG ID
    dag = dag_bag.get_dag(dag_id)

    # Ensure the DAG exists
    assert dag is not None, f"DAG with ID {dag_id} not found."

    test_mode = conf.getboolean("core", "unit_test_mode")
    print(f"x: test_mode: {test_mode}")

    dag_dir = conf.get("core", "dags_folder")
    print(f"x: dag_dir: {dag_dir}")

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
    executor.parallelism = 5
    executor.name = ExecutorName(module_path="airflow.executors.local_executor.LocalExecutor", alias="LocalExecutor")
    executor.callback_sink = PipeCallbackSink(get_sink_pipe= processor_agent.get_callbacks_pipe)
    executor.start()

    tis = (task for task in dag.task_dict.values())
    for ti in tis:
        # print(f"ti.dag_id: {ti.dag_id}")
        # print(f"ti.task_id: {ti.task_id}")
        # print(f"ti.run_id: {ti.run_id}")
        ti.map_index = -1
        ti.try_number = 1
        ti.state = TaskInstanceState.QUEUED

        print(f"name: {executor.name}")
        print(f"name: {executor.name.alias}")
        ti.executor = str(executor.name)

        execution_date = now
        run_type = DagRunType.MANUAL
        data_interval = dag.timetable.infer_manual_data_interval(run_after=execution_date)
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dagrun = dag.create_dagrun(
            run_type=run_type,
            execution_date=execution_date,
            data_interval=data_interval,
            run_id=str("abc1232_" + ti.task_id),
            start_date=now,
            state=TaskInstanceState.QUEUED,
            external_trigger=False,
            **triggered_by_kwargs
        )
        ti.dag_run = dagrun
        ti.run_id = dagrun.run_id

        task_instance_key = TaskInstanceKey(dag_id=ti.dag_id, task_id=ti.task_id, run_id=ti.run_id, try_number=ti.try_number, map_index=ti.map_index)
        # key = ("fail", "test_simple_ti", now, 1)
        # ti.key = key
        ti.key = task_instance_key

        value_tuple = (
            "command",
            1,
            None,
            ti
            # SimpleTaskInstance.from_ti(ti=ti),
        )
        executor.queued_tasks[task_instance_key] = value_tuple
        # executor.task_publish_retries[task_instance_key] = 1

    # executor.heartbeat()

    # assert 0 == len(executor.queued_tasks), "Task should no longer be queued"
    # assert executor.event_buffer[("fail", "fake_simple_ti", when, 0)][0] == State.FAILED

    job = Job(executor=executor)
    schedulerJobRunner = SchedulerJobRunner(job=job, num_times_parse_dags=1)

    schedulerJobRunner.processor_agent = processor_agent
    schedulerJobRunner.processor_agent.start()

    # do_scheduling
    schedulerJobRunner._run_scheduler_loop()
    print("x: Out of the loop.")
    # schedulerJobRunner.processor_agent.end()
    # executor.end(synchronous=True)
    print("x: After.")
    # dagrun = dag.create_dagrun()

    # dagrun = dag.test(execution_date=now)
    #

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

    # Example: Check that the span has the correct attributes
    # assert captured_spans[0].name == "test_span", "Captured span name does not match."
    # assert captured_spans[0].attributes[
    #            "test_attribute"] == "test_value", "Captured span attribute does not match."
