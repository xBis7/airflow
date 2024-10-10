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

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.traces import otel_tracer
from airflow.traces.tracer import Trace
from datetime import datetime
from opentelemetry import trace

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

    airflow_otel_tracer = otel_tracer.get_otel_tracer(Trace)
    print(f"type: {type(airflow_otel_tracer)}")

    # pydevd_pycharm.settrace('host.docker.internal', port=3003, stdoutToServer=True, stderrToServer=True)

    # tracer = trace.get_tracer("trace_test.tracer",tracer_provider=otel_hook.tracer_provider)
    tracer = airflow_otel_tracer.get_tracer("trace_test.tracer")

    # Task functions
    def task_1_func(**dag_context):

      print(f"dag_context: {dag_context}")

      current_context0 = trace.get_current_span().get_span_context()
      airflow_current_context0 = airflow_otel_tracer.get_current_span().get_span_context()

      dag_run = dag_context["dag_run"]
      task_instance = dag_context["ti"]
      print(f"xbis: task_instance: {task_instance.context_carrier}")

      print(f"curr_t_id: {current_context0.trace_id} | curr_s_id: {current_context0.span_id}")
      print(f"airf_curhr_t_id: {airflow_current_context0.trace_id} | airf_curr_s_id: {airflow_current_context0.span_id}")

      # pydevd_pycharm.settrace('host.docker.internal', port=3003, stdoutToServer=True, stderrToServer=True)
      # print("Waiting for debugger attach...")

      # Start a span and print the context before injecting
      with airflow_otel_tracer.start_span_from_taskinstance(ti=task_instance, span_name="task_1_span", child = True) as s1:
          print("Task_1, first part")

          # Start a new span using the extracted context
          with airflow_otel_tracer.start_span("extracted_span") as span2:
              for i in range(5):
                  # time.sleep(3)
                  print(f"Task_1, iteration '{i}' with context propagation")
              print("Task_1 finished")

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
