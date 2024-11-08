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
import os
import subprocess
import time
from importlib import reload

import pendulum
import pytest
from airflow.cli import cli_parser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from airflow import settings
from airflow.configuration import conf
from airflow.executors import executor_loader
from airflow.executors.executor_utils import ExecutorName
from airflow.models import DagBag, DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.celery.executors import celery_executor
from airflow.traces.otel_tracer import CTX_PROP_SUFFIX
from airflow.utils.db import resetdb
from airflow.utils.session import create_session
from airflow.utils.state import State
from tests.otel.test_utils import (
    extract_spans_from_output,
    get_parent_child_dict,
    assert_span_name_belongs_to_root_span,
    assert_parent_children_spans,
    assert_span_not_in_children_spans,
    assert_parent_children_spans_for_non_root,
    dump_airflow_metadata_db
)
from tests.test_utils.config import conf_vars

log = logging.getLogger("test_otel")

@pytest.mark.integration("celery")
@pytest.mark.backend("postgres")
class TestOtelIntegration:

    @classmethod
    def setup_class(cls):
        with conf_vars({("core", "executor"): "CeleryExecutor"}):
            # The cli_parser module is loaded during test collection. Reload it here with the
            # executor overridden so that we get the expected commands loaded.
            reload(executor_loader)
            reload(cli_parser)
            cls.parser = cli_parser.get_parser()

    def setup_method(self):
        dag_folder = conf.get("core", "DAGS_FOLDER")

        # Load DAGs from that directory
        self.dag_bag = DagBag(dag_folder=dag_folder, include_examples=False)

    @conf_vars(
        {
            ("core", "executor"): "CeleryExecutor",
        }
    )
    def test_dag_spans_with_context_propagation(
        self,
        capfd,
        session):
        """
        Test that a DAG runs successfully and exports the correct spans,
        using a scheduler, a celery worker, a postgres db and a redis broker.
        """
        # Uncomment to enable debug mode and get span and db dumps on the output.
        log.setLevel(logging.DEBUG)

        execution_date = pendulum.now("UTC")

        dag_id = "test_dag"
        dag = self.dag_bag.get_dag(dag_id)

        assert dag is not None, f"DAG with ID {dag_id} not found."

        with create_session() as session:
            # Sync the DAG to the database.
            dag.sync_to_db(session=session)
            # Manually serialize the dag and write it to the db to avoid a db error.
            SerializedDagModel.write_dag(dag, session=session)
            session.commit()

        unpause_command = [
            "airflow",
            "dags",
            "unpause",
            dag_id
        ]

        # Unpause the dag using the cli.
        subprocess.run(unpause_command, check=True, env=os.environ.copy())

        run_id = f"manual__{execution_date.isoformat()}"

        trigger_command = [
            "airflow",
            "dags",
            "trigger",
            dag_id,
            "--run-id", run_id,
            "--exec-date", execution_date.isoformat(),
        ]

        # Trigger the dag using the cli.
        subprocess.run(trigger_command, check=True, env=os.environ.copy())

        # Wait timeout for the DAG run to complete.
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
                log.info(f"DAG Run state: {dag_run_state}.")

                if dag_run_state in [State.SUCCESS, State.FAILED]:
                    break

            time.sleep(5)

        if logging.root.level == logging.DEBUG:
            with create_session() as session:
                dump_airflow_metadata_db(session)

        assert dag_run_state == State.SUCCESS, f"DAG run did not complete successfully. Final state: {dag_run_state}."

        out, err = capfd.readouterr()
        log.debug(f"out-start --\n{out}\n-- out-end")
        log.debug(f"err-start --\n{err}\n-- err-end")

        # output_lines = out.splitlines()
        #
        # root_span_dict, span_dict = extract_spans_from_output(output_lines)
        # parent_child_dict = get_parent_child_dict(root_span_dict, span_dict)
        #
        # dag_span_name = str(dag_id + CTX_PROP_SUFFIX)
        # assert_span_name_belongs_to_root_span(root_span_dict=root_span_dict, span_name=dag_span_name,
        #                                       should_succeed=True)
        #
        # non_existent_dag_span_name = str(dag_id + CTX_PROP_SUFFIX + "fail")
        # assert_span_name_belongs_to_root_span(root_span_dict=root_span_dict, span_name=non_existent_dag_span_name,
        #                                       should_succeed=False)
        #
        # dag_children_span_names = []
        # task_instance_ids = dag.task_ids
        #
        # for task_id in task_instance_ids:
        #     dag_children_span_names.append(f"{task_id}{CTX_PROP_SUFFIX}")
        #
        # first_task_id = task_instance_ids[0]
        #
        # assert_parent_children_spans(parent_child_dict=parent_child_dict, root_span_dict=root_span_dict,
        #                              parent_name=dag_span_name, children_names=dag_children_span_names)
        #
        # assert_span_not_in_children_spans(parent_child_dict=parent_child_dict, root_span_dict=root_span_dict,
        #                                   span_dict=span_dict,
        #                                   parent_name=dag_span_name, child_name=first_task_id, span_exists=True)
        #
        # assert_span_not_in_children_spans(parent_child_dict=parent_child_dict, root_span_dict=root_span_dict,
        #                                   span_dict=span_dict,
        #                                   parent_name=dag_span_name, child_name=f"{first_task_id}_fail",
        #                                   span_exists=False)
        #
        # # Any spans generated under a task, are children of the task span.
        # # The span hierarchy for dag 'test_dag' is
        # # dag span
        # #   |_ task_1 span
        # #       |_ sub_span_1
        # #           |_ sub_span_2
        # #               |_ sub_span_3
        # #       |_ sub_span_4
        # #   |_ task_2 span
        #
        # first_task_children_span_names = [
        #     f"{first_task_id}_sub_span1{CTX_PROP_SUFFIX}",
        #     f"{first_task_id}_sub_span4{CTX_PROP_SUFFIX}"
        # ]
        # assert_parent_children_spans_for_non_root(span_dict=span_dict,
        #                                           parent_name=f"{first_task_id}{CTX_PROP_SUFFIX}",
        #                                           children_names=first_task_children_span_names)
        #
        # # Single element list.
        # sub_span1_children_span_names = [
        #     f"{first_task_id}_sub_span2{CTX_PROP_SUFFIX}"
        # ]
        # assert_parent_children_spans_for_non_root(span_dict=span_dict,
        #                                           parent_name=f"{first_task_id}_sub_span1{CTX_PROP_SUFFIX}",
        #                                           children_names=sub_span1_children_span_names)
        #
        # sub_span2_children_span_names = [
        #     f"{first_task_id}_sub_span3{CTX_PROP_SUFFIX}"
        # ]
        # assert_parent_children_spans_for_non_root(span_dict=span_dict,
        #                                           parent_name=f"{first_task_id}_sub_span2{CTX_PROP_SUFFIX}",
        #                                           children_names=sub_span2_children_span_names)
