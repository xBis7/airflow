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
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanContext, NonRecordingSpan

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.traces import otel_tracer
from airflow.traces.otel_tracer import OtelTrace
from airflow.traces.tracer import Trace
from datetime import datetime
from opentelemetry import trace

import time

args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 1),
    'retries': 0,
}

# Define the DAG (Directed Acyclic Graph)
with DAG(
    'test_dag',
    default_args=args,
    schedule=None,
    catchup=False,
) as dag:

    # memory_exporter = InMemorySpanExporter()
    # airflow_otel_tracer = OtelTrace(span_exporter=memory_exporter)

    # tracer = airflow_otel_tracer.get_tracer("trace_test.tracer")

    # print(f"type: {type(airflow_otel_tracer)}")

    # pydevd_pycharm.settrace('host.docker.internal', port=3003, stdoutToServer=True, stderrToServer=True)

    # Task functions
    def task_1_func(**dag_context):
        print(f"dag_context: {dag_context}")

        # print(f"xbis: __carrier: {airflow_otel_tracer.carrier}")

        current_context0 = trace.get_current_span().get_span_context()
        # airflow_current_context0 = airflow_otel_tracer.get_current_span().get_span_context()

        dag_run = dag_context["dag_run"]
        ti = dag_context["ti"]
        task_instance = dag_context["task_instance"]
        carrier_from_context = dag_context["carrier"]

        if carrier_from_context is not None:
            context = Trace.extract(carrier_from_context)
            print(f"xbis: context: {context}")
            for key, value in context.items():
                if isinstance(value, NonRecordingSpan):
                    span_context = value.get_span_context()

                    # Extract trace_id, span_id, etc. from SpanContext
                    trace_id = span_context.trace_id
                    span_id = span_context.span_id
                    trace_flags = span_context.trace_flags
                    trace_state = span_context.trace_state

                    print(f"Extracted SpanContext from key '{key}':")
                    print(f"  trace_id: {hex(trace_id)}")
                    print(f"  span_id: {hex(span_id)}")
                    print(f"  trace_flags: {trace_flags}")

                    with Trace.start_span(span_name="task1_sub_span", component="taskinstance", parent_sc=span_context) as span:
                        span.set_attribute("what?", "hi")
                    break
            else:
                raise ValueError("No valid NonRecordingSpan found in the context")

            # span_ctx = SpanContext(
            #     trace_id=context.get("tra"), span_id=INVALID_SPAN_ID, is_remote=True,
            #     trace_flags=TraceFlags(0x01)
            # )
            # ctx = trace.set_span_in_context(NonRecordingSpan(span_ctx))
            # span = tracer.start_as_current_span(
            #
            # with airflow_otel_tracer.start_span(span_name="task1_sub_span", component="taskinstance", parent_sc=)

        print(
            f"xbis: carrier_from_context: {carrier_from_context} | ti.carrier: {ti.carrier} | task_instance.carrier: {task_instance.carrier}")

        print(f"curr_t_id: {current_context0.trace_id} | curr_s_id: {current_context0.span_id}")
        # print(f"airf_curhr_t_id: {airflow_current_context0.trace_id} | airf_curr_s_id: {airflow_current_context0.span_id}")


    def task_2_func():
        for i in range(3):
            print(f"Task_2, iteration '{i}'")
        print("Task_2 finished")

    # Setup PythonOperator tasks
    t1 = PythonOperator(
        task_id='task_1',
        python_callable=task_1_func,
    )

    t2 = PythonOperator(
        task_id='task_2',
        python_callable=task_2_func,
    )

    t1 >> t2
