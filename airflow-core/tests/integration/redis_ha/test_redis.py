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
from __future__ import annotations

import json
import logging
import os
import subprocess
import time

import pytest

from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.executors import executor_loader
from airflow.executors.executor_utils import ExecutorName
from airflow.models import DAG, DagBag, DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

log = logging.getLogger("integration.redis-ha.test_redis")


def unpause_trigger_dag_and_get_run_id(dag_id: str) -> str:
    unpause_command = ["airflow", "dags", "unpause", dag_id]

    # Unpause the dag using the cli.
    subprocess.run(unpause_command, check=True, env=os.environ.copy())

    execution_date = timezone.utcnow()
    run_id = f"manual__{execution_date.isoformat()}"

    trigger_command = [
        "airflow",
        "dags",
        "trigger",
        dag_id,
        "--run-id",
        run_id,
        "--logical-date",
        execution_date.isoformat(),
    ]

    # Trigger the dag using the cli.
    subprocess.run(trigger_command, check=True, env=os.environ.copy())

    return run_id


def wait_for_dag_run(dag_id: str, run_id: str, max_wait_time: int):
    # max_wait_time, is the timeout for the DAG run to complete. The value is in seconds.
    start_time = timezone.utcnow().timestamp()

    while timezone.utcnow().timestamp() - start_time < max_wait_time:
        with create_session() as session:
            dag_run = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == dag_id,
                    DagRun.run_id == run_id,
                )
                .first()
            )

            if dag_run is None:
                time.sleep(5)
                continue

            dag_run_state = dag_run.state
            log.debug("DAG Run state: %s.", dag_run_state)

            dag_run_span_status = dag_run.span_status
            log.debug("DAG Run span status: %s.", dag_run_span_status)

            if dag_run_state in [State.SUCCESS, State.FAILED]:
                break

    assert dag_run_state == State.SUCCESS, (
        f"Dag run did not complete successfully. Final state: {dag_run_state}."
    )


def check_dag_run_state(dag_id: str, run_id: str, state: str):
    with create_session() as session:
        dag_run = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == dag_id,
                DagRun.run_id == run_id,
            )
            .first()
        )

        assert dag_run.state == state, f"Dag Run state isn't {state}. State: {dag_run.state}"


def check_ti_state(task_id: str, run_id: str, state: str):
    with create_session() as session:
        ti = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.task_id == task_id,
                TaskInstance.run_id == run_id,
            )
            .first()
        )

        assert ti.state == state, f"Task instance state isn't {state}. State: {ti.state}"


def print_ti_output_for_dag_run(dag_id: str, run_id: str):
    breeze_logs_dir = "/root/airflow/logs"

    # For structured logs, the path is:
    #   '/root/airflow/logs/dag_id=.../run_id=.../task_id=.../attempt=1.log'
    # TODO: if older airflow versions start throwing errors,
    #   then check if the path needs to be adjusted to something like
    #   '/root/airflow/logs/<dag_id>/<task_id>/<run_id>/...'
    dag_run_path = os.path.join(breeze_logs_dir, f"dag_id={dag_id}", f"run_id={run_id}")

    for root, _dirs, files in os.walk(dag_run_path):
        for filename in files:
            if filename.endswith(".log"):
                full_path = os.path.join(root, filename)
                log.info("\n===== LOG FILE: %s - START =====\n", full_path)
                try:
                    with open(full_path) as f:
                        log.info(f.read())
                except Exception as e:
                    log.error("Could not read %s: %s", full_path, e)

                log.info("\n===== END =====\n")


@pytest.mark.integration("redis_ha")
@pytest.mark.backend("postgres")
class TestRedisIntegration:
    test_dir = os.path.dirname(os.path.abspath(__file__))
    dag_folder = os.path.join(test_dir, "dags")

    max_wait_seconds_for_pause = 180

    celery_command_args = [
        "celery",
        "--app",
        "airflow.providers.celery.executors.celery_executor.app",
        "worker",
        "--concurrency",
        "1",
        "--loglevel",
        "DEBUG",
    ]

    scheduler_command_args = [
        "airflow",
        "scheduler",
    ]

    apiserver_command_args = [
        "airflow",
        "api-server",
        "--port",
        "8080",
        "--daemon",
    ]

    dags: dict[str, DAG] = {}

    @classmethod
    def setup_class(cls):
        os.environ["AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR"] = "False"
        os.environ["AIRFLOW__SCHEDULER__PROCESSOR_POLL_INTERVAL"] = "2"

        # The heartrate is determined by the conf "AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC".
        # By default, the heartrate is 5 seconds. Every iteration of the scheduler loop, checks the
        # time passed since the last heartbeat and if it was longer than the 5 second heartrate,
        # it performs a heartbeat update.
        # If there hasn't been a heartbeat for an amount of time longer than the
        # SCHEDULER_HEALTH_CHECK_THRESHOLD, then the scheduler is considered unhealthy.
        # Approximately, there is a scheduler heartbeat every 5-6 seconds. Set the threshold to 15.
        os.environ["AIRFLOW__SCHEDULER__SCHEDULER_HEALTH_CHECK_THRESHOLD"] = "15"

        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{cls.dag_folder}"

        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = "/dev/null"
        os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"

        # os.environ["AIRFLOW__CELERY__BROKER_URL"] = (
        #     "sentinel://sentinel-1:26379;sentinel://sentinel-2:26379;sentinel://sentinel-3:26379/0"
        # )
        # os.environ["AIRFLOW__CELERY__RESULT_BACKEND"] = "db+postgresql://postgres:airflow@postgres/airflow"
        # os.environ["AIRFLOW__CELERY_BROKER_TRANSPORT_OPTIONS__MASTER_NAME"] = "test-cluster"

        os.environ["AIRFLOW__CELERY__CELERY_CONFIG_OPTIONS"] = (
            "tests_common.test_utils.celery_custom_config.CONFIG_WITH_RETRY_POLICY"
        )

        # os.environ["AIRFLOW__LOGGING__LOGGING_LEVEL"] = "DEBUG"

    @classmethod
    def serialize_and_get_dags(cls) -> dict[str, DAG]:
        log.info("Serializing Dags from directory %s", cls.dag_folder)
        # Load DAGs from the dag directory.
        dag_bag = DagBag(dag_folder=cls.dag_folder, include_examples=False)

        dag_ids = dag_bag.dag_ids
        assert len(dag_ids) == 1

        dag_dict: dict[str, DAG] = {}
        with create_session() as session:
            for dag_id in dag_ids:
                dag = dag_bag.get_dag(dag_id)
                dag_dict[dag_id] = dag

                assert dag is not None, f"DAG with ID {dag_id} not found."

                # Sync the DAG to the database.
                if AIRFLOW_V_3_0_PLUS:
                    from airflow.models.dagbundle import DagBundleModel

                    if session.query(DagBundleModel).filter(DagBundleModel.name == "testing").count() == 0:
                        session.add(DagBundleModel(name="testing"))
                        session.commit()
                    dag.bulk_write_to_db(
                        bundle_name="testing", bundle_version=None, dags=[dag], session=session
                    )
                else:
                    dag.sync_to_db(session=session)
                # Manually serialize the dag and write it to the db to avoid a db error.
                SerializedDagModel.write_dag(dag, bundle_name="testing", session=session)

            session.commit()

        TESTING_BUNDLE_CONFIG = [
            {
                "name": "testing",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": f"{cls.dag_folder}", "refresh_interval": 1},
            }
        ]

        os.environ["AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST"] = json.dumps(TESTING_BUNDLE_CONFIG)
        # Initial add
        manager = DagBundlesManager()
        manager.sync_bundles_to_db()

        return dag_dict

    @pytest.fixture
    def celery_worker_env_vars(self, monkeypatch):
        os.environ["AIRFLOW__CORE__EXECUTOR"] = "CeleryExecutor"
        executor_name = ExecutorName(
            module_path="airflow.providers.celery.executors.celery_executor.CeleryExecutor",
            alias="CeleryExecutor",
        )
        monkeypatch.setattr(executor_loader, "_alias_to_executors", {"CeleryExecutor": executor_name})

    @pytest.fixture(autouse=True)
    def reset_db(self):
        reset_command = ["airflow", "db", "reset", "--yes"]

        # Reset the db using the cli.
        subprocess.run(reset_command, check=True, env=os.environ.copy())

        migrate_command = ["airflow", "db", "migrate"]
        subprocess.run(migrate_command, check=True, env=os.environ.copy())

        self.dags = self.serialize_and_get_dags()

    def test_dag_execution_succeeds(self, monkeypatch, celery_worker_env_vars, capfd, session):
        """The same scheduler will start and finish the dag processing."""
        celery_worker_process = None
        scheduler_process = None
        apiserver_process = None
        try:
            # Start the processes here and not as fixtures or in a common setup,
            # so that the test can capture their output.
            celery_worker_process, scheduler_process, apiserver_process = self.start_worker_and_scheduler1()

            dag_id = "test_dag"

            assert len(self.dags) > 0
            dag = self.dags[dag_id]

            assert dag is not None

            run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_id)

            wait_for_dag_run(dag_id=dag_id, run_id=run_id, max_wait_time=200)

            time.sleep(10)

            with create_session() as session:
                tis: list[TaskInstance] = dag.get_task_instances(session=session)

            for ti in tis:
                check_ti_state(task_id=ti.task_id, run_id=run_id, state=State.SUCCESS)

            print_ti_output_for_dag_run(dag_id=dag_id, run_id=run_id)
        finally:
            # Terminate the processes.
            celery_worker_process.terminate()
            celery_worker_process.wait()

            celery_status = celery_worker_process.poll()
            assert celery_status is not None, (
                "The celery worker process status is None, which means that it hasn't terminated as expected."
            )

            scheduler_process.terminate()
            scheduler_process.wait()

            scheduler_status = scheduler_process.poll()
            assert scheduler_status is not None, (
                "The scheduler_1 process status is None, which means that it hasn't terminated as expected."
            )

            apiserver_process.terminate()
            apiserver_process.wait()

            apiserver_status = apiserver_process.poll()
            assert apiserver_status is not None, (
                "The apiserver process status is None, which means that it hasn't terminated as expected."
            )

        out, err = capfd.readouterr()
        log.info("out-start --\n%s\n-- out-end", out)
        log.info("err-start --\n%s\n-- err-end", err)

    def start_worker_and_scheduler1(self):
        celery_worker_process = subprocess.Popen(
            self.celery_command_args,
            env=os.environ.copy(),
            stdout=None,
            stderr=None,
        )

        scheduler_process = subprocess.Popen(
            self.scheduler_command_args,
            env=os.environ.copy(),
            stdout=None,
            stderr=None,
        )

        apiserver_process = subprocess.Popen(
            self.apiserver_command_args,
            env=os.environ.copy(),
            stdout=None,
            stderr=None,
        )

        # Wait to ensure both processes have started.
        time.sleep(10)

        return celery_worker_process, scheduler_process, apiserver_process
