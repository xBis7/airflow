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

from airflow.traces import otel_tracer

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.traces.otel_tracer import CTX_PROP_SUFFIX
from airflow.traces.tracer import Trace
from datetime import datetime
from opentelemetry import trace

# CTX_PROP_SUFFIX="_ctx_prop"

args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 1),
    'retries': 0,
}

# Define the DAG.
with DAG(
    'test_dag',
    default_args=args,
    schedule=None,
    catchup=False,
) as dag:

    # Task functions
    def task_1_func(**dag_context):
        print(f"dag_context: {dag_context}")

        current_context0 = trace.get_current_span().get_span_context()

        ti = dag_context["ti"]

        otel_airflow_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
        context_carrier = ti.context_carrier

        # with otel_airflow_tracer.start_root_span(span_name=f"{ti.task_id}_span_from_inside_without_x", component="dag_x") as s:
        #   print(f"xbis: context from current_context")
        #   print("halo")

        tracer_provider = otel_airflow_tracer.get_otel_tracer_provider()

        if context_carrier is not None:
            parent_context = Trace.extract(context_carrier)
            with otel_airflow_tracer.start_child_span(span_name=f"{ti.task_id}_sub_span1{CTX_PROP_SUFFIX}",
                                          parent_context=parent_context, component=f"dag{CTX_PROP_SUFFIX}") as s1:
                print(f"xbis: context: {parent_context}")
                print("halo")

                with otel_airflow_tracer.start_child_span(f"{ti.task_id}_sub_span2{CTX_PROP_SUFFIX}") as s2:
                    print("halo2")

                    tracer = trace.get_tracer("trace_test.tracer", tracer_provider=tracer_provider)
                    with tracer.start_as_current_span(name=f"{ti.task_id}_sub_span3{CTX_PROP_SUFFIX}") as s3:
                        print("halo3")
            with otel_airflow_tracer.start_child_span(span_name=f"{ti.task_id}_sub_span4{CTX_PROP_SUFFIX}",
                                                      parent_context=parent_context, component=f"dag{CTX_PROP_SUFFIX}") as s4:
                print("halo4")

        print(f"xbis: context_carrier: {context_carrier}")

        print(f"curr_t_id: {current_context0.trace_id} | curr_s_id: {current_context0.span_id}")

    def task_2_func():
        for i in range(3):
            print(f"Task_2, iteration '{i}'")
        print("Task_2 finished")

    # def task_3_func(task_id, carrier):
    #     import sys
    #
    #     print(sys.version)
    #
    #     import ast
    #     # import logging
    #     from airflow.traces import otel_tracer
    #     from airflow.traces.tracer import Trace
    #     from opentelemetry import trace
    #
    #     CTX_PROP_SUFFIX="_ctx_prop"
    #
    #     # get the airflow.task logger
    #     # task_logger = logging.getLogger("airflow.task")
    #
    #     # get the Open Telemetry hook
    #     otel_airflow_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    #
    #     print("Starting beta_impl")
    #
    #     # context_carrier is passed as a string. Convert it back to a dictionary.
    #     context_carrier = ast.literal_eval(carrier)
    #
    #     tracer_provider = otel_airflow_tracer.get_otel_tracer_provider()
    #
    #     if context_carrier is not None:
    #         parent_context = Trace.extract(context_carrier)
    #         with otel_airflow_tracer.start_child_span(span_name=f"{task_id}_sub_span1{CTX_PROP_SUFFIX}",
    #                                                   parent_context=parent_context,
    #                                                   component=f"dag{CTX_PROP_SUFFIX}") as s1:
    #             print(f"xbis: context: {parent_context}")
    #             print("halo")
    #
    #             with otel_airflow_tracer.start_child_span(f"{task_id}_sub_span2{CTX_PROP_SUFFIX}") as s2:
    #                 print("halo2")
    #
    #                 tracer = trace.get_tracer("trace_test.tracer", tracer_provider=tracer_provider)
    #                 with tracer.start_as_current_span(name=f"{task_id}_sub_span3{CTX_PROP_SUFFIX}") as s3:
    #                     print("halo3")
    #         with otel_airflow_tracer.start_child_span(span_name=f"{task_id}_sub_span4{CTX_PROP_SUFFIX}",
    #                                                   parent_context=parent_context,
    #                                                   component=f"dag{CTX_PROP_SUFFIX}") as s4:
    #             print("halo4")

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

    # t3 = PythonVirtualenvOperator(
    #     task_id='task_3',
    #     python_callable=task_3_func,
    #     op_kwargs={
    #         'task_id': '{{ ti.task_id }}',
    #         'carrier': '{{ ti.context_carrier }}',
    #     },
    #     requirements=[
    #         'dill',
    #         'pyre2',
    #         'typing-extensions',
    #         'methodtools',
    #         'PyYAML',
    #         'sqlalchemy',
    #         'sqlalchemy-jsonfield',
    #         'requests',
    #         'tenacity',
    #         'pluggy',
    #         'pendulum',
    #         'jwt',
    #         'colorlog',
    #         'lazy_object_proxy',
    #         'jinja2',
    #         'attrs',
    #         'alembic',
    #         'apache-airflow @ file:///Users/xbis/projects/work/airflow'
    #     ],
    #     system_site_packages=False,
    #     serializer='dill'
    # )
    #
    # t1 >> t2 >> t3
