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
from pathlib import Path

import pytest

from tests_common.test_utils.dag import sync_dag_to_db

os.environ["AIRFLOW__METRICS__OTEL_ON"] = "True"
os.environ["AIRFLOW__METRICS__OTEL_HOST"] = "breeze-otel-collector"
os.environ["AIRFLOW__METRICS__OTEL_PORT"] = "4318"
os.environ["AIRFLOW__METRICS__OTEL_INTERVAL_MILLISECONDS"] = "1000"
# Import here so that it can read the environment variables and use the correct implementation.
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.executors import executor_loader
from airflow.executors.executor_utils import ExecutorName
from airflow.models import DAG, DagBag, DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import State

try:
    from airflow.sdk import timezone
except ImportError:
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]

log = logging.getLogger("integration.perf.test_perf")

metrics_file = Path(__file__).parent / "airflow_perf_metrics.json"


@pytest.fixture(scope="session", autouse=True)
def rotate_metrics_file_once():
    """
    If airflow_perf_metrics.json already exists, move it to
    airflow_perf_metrics_old.json.  Overwrite any previous *_old file.
    """
    if metrics_file.exists():
        backup = metrics_file.with_stem(metrics_file.stem + "_old")
        metrics_file.replace(backup)


def unpause_trigger_dag_and_get_run_id(dag_id: str, unpause: bool = True) -> str:
    """
    Running this multiple times for the same dag without unpause, will create multiple dag_runs.
    """
    if unpause:
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


@pytest.mark.integration("otel")
@pytest.mark.integration("redis")
@pytest.mark.backend("postgres")
class TestPerformanceIntegration:
    test_dir = os.path.dirname(os.path.abspath(__file__))
    # TODO: adjust the last folder to avoid loading everything. Or remove it.
    dag_folder = os.path.join(test_dir, "dags")

    dag_num = os.getenv("dag_num", default="2")
    log_level = os.getenv("log_level", default="none")

    celery_command_args = [
        "celery",
        "--app",
        "airflow.providers.celery.executors.celery_executor.app",
        "worker",
        "--concurrency",
        "30",
        "--pool",
        "prefork",
        "--loglevel",
        "INFO",
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
        os.environ["AIRFLOW_SCHEDULER__RUNNING_METRICS_INTERVAL"] = "5"
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

        # How many the scheduler can schedule at once.
        os.environ["AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY"] = "100"
        os.environ["AIRFLOW__SCHEDULER__MAX_DAGRUNS_TO_CREATE_PER_LOOP"] = "10"
        os.environ["AIRFLOW__SCHEDULER__MAX_DAGRUNS_PER_LOOP_TO_SCHEDULE"] = "20"
        os.environ["AIRFLOW__SCHEDULER__PARSING_PROCESSES"] = "2"

        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{cls.dag_folder}"

        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = "/dev/null"
        os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"

        # Max number of tasks that can run concurrently per scheduler.
        # e.g. if parallelism is 32, for 2 schedulers the number will be 32 * 2 = 64
        os.environ["AIRFLOW__CORE__PARALLELISM"] = "100"
        # Number of tasks that can run concurrently per dag.
        os.environ["AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG"] = "4"
        # Number of active dag_runs per dag.
        os.environ["AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG"] = "10"
        os.environ["AIRFLOW__CORE__DEFAULT_POOL_TASK_SLOT_COUNT"] = "64"

        os.environ["AIRFLOW__METRICS__OTEL_ON"] = "True"
        os.environ["AIRFLOW__METRICS__OTEL_HOST"] = "breeze-otel-collector"
        os.environ["AIRFLOW__METRICS__OTEL_PORT"] = "4318"
        os.environ["AIRFLOW__METRICS__OTEL_INTERVAL_MILLISECONDS"] = "1000"

        # os.environ["AIRFLOW__CELERY__WORKER_CONCURRENCY"] = "100"
        os.environ["AIRFLOW__CELERY__EXTRA_CELERY_CONFIG"] = '{"worker_max_tasks_per_child": 100}'
        os.environ["AIRFLOW__CELERY__WORKER_PREFETCH_MULTIPLIER"] = "1"
        os.environ["AIRFLOW__CELERY__SYNC_PARALLELISM"] = "0"
        os.environ["AIRFLOW__CELERY__OPERATION_TIMEOUT"] = "300"
        os.environ["AIRFLOW__CELERY__TASK_PUBLISH_MAX_RETRIES"] = "3"

        if cls.log_level == "debug":
            log.setLevel(logging.DEBUG)

        # Reset the DB once at the beginning and serialize the dags.
        reset_command = ["airflow", "db", "reset", "--yes"]
        subprocess.run(reset_command, check=True, env=os.environ.copy())

        migrate_command = ["airflow", "db", "migrate"]
        subprocess.run(migrate_command, check=True, env=os.environ.copy())

        cls.dags = cls.serialize_and_get_dags()

    @classmethod
    def serialize_and_get_dags(cls) -> dict[str, DAG]:
        log.info("Serializing Dags from directory %s", cls.dag_folder)
        # Load DAGs from the dag directory.
        dag_bag = DagBag(dag_folder=cls.dag_folder, include_examples=False)

        dag_ids = dag_bag.dag_ids
        assert len(dag_ids) > 0

        dag_dict: dict[str, DAG] = {}
        with create_session() as session:
            for dag_id in dag_ids:
                dag = dag_bag.get_dag(dag_id)
                dag_dict[dag_id] = dag

                assert dag is not None, f"DAG with ID {dag_id} not found."

                sync_dag_to_db(dag)
                # Sync the DAG to the database.
                # from airflow.models.dagbundle import DagBundleModel
                #
                # if session.query(DagBundleModel).filter(DagBundleModel.name == "testing").count() == 0:
                #     session.add(DagBundleModel(name="testing"))
                #     session.commit()
                # SerializedDAG.bulk_write_to_db(bundle_name="testing", bundle_version=None, dags=[dag], session=session)
                # Manually serialize the dag and write it to the db to avoid a db error.
                # SerializedDagModel.write_dag(dag, bundle_name="testing", session=session)

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
        monkeypatch.setattr(
            executor_loader, "_alias_to_executors_per_team", {"CeleryExecutor": executor_name}
        )

    @pytest.mark.parametrize(
        "flag_enabled", [pytest.param("True", id="with_fts"), pytest.param("False", id="fts_disabled")]
    )
    def test_metrics(self, flag_enabled: str, monkeypatch, celery_worker_env_vars, capfd, session):
        os.environ["AIRFLOW__SCHEDULER__ENABLE_FAIR_TASK_SELECTION"] = flag_enabled

        scheduler_1_process = None

        celery_worker_1_process = None
        celery_worker_2_process = None
        celery_worker_3_process = None

        apiserver_process = None

        dag_10_id = "dag_10_tasks"

        dag_10_run_id = None
        try:
            # Start the processes here and not as fixtures or in a common setup,
            # so that the test can capture their output.
            (
                scheduler_1_process,
                celery_worker_1_process,
                celery_worker_2_process,
                celery_worker_3_process,
                apiserver_process,
            ) = self.start_schedulers_and_workers(second_sched=False)

            assert len(self.dags) > 0
            dag_10 = self.dags[dag_10_id]

            assert dag_10 is not None

            dag_10_run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_10_id)
            dag_10_run_id2 = unpause_trigger_dag_and_get_run_id(dag_id=dag_10_id, unpause=False)

            wait_for_dag_run(dag_id=dag_10_id, run_id=dag_10_run_id, max_wait_time=900)
            wait_for_dag_run(dag_id=dag_10_id, run_id=dag_10_run_id2, max_wait_time=900)

            time.sleep(10)
        finally:
            if dag_10_run_id is not None:
                print_ti_output_for_dag_run(dag_id=dag_10_id, run_id=dag_10_run_id)
            if dag_10_run_id2 is not None:
                print_ti_output_for_dag_run(dag_id=dag_10_id, run_id=dag_10_run_id2)

            # Terminate the processes.
            if celery_worker_1_process is not None:
                celery_worker_1_process.terminate()
                celery_worker_1_process.wait()

                celery_1_status = celery_worker_1_process.poll()
                assert celery_1_status is not None, (
                    "The celery_worker_1 process status is None, which means that it hasn't terminated as expected."
                )

            if celery_worker_2_process is not None:
                celery_worker_2_process.terminate()
                celery_worker_2_process.wait()

                celery_2_status = celery_worker_2_process.poll()
                assert celery_2_status is not None, (
                    "The celery_worker_2 process status is None, which means that it hasn't terminated as expected."
                )

            if celery_worker_3_process is not None:
                celery_worker_3_process.terminate()
                celery_worker_3_process.wait()

                celery_3_status = celery_worker_3_process.poll()
                assert celery_3_status is not None, (
                    "The celery_worker_3 process status is None, which means that it hasn't terminated as expected."
                )

            if scheduler_1_process is not None:
                scheduler_1_process.terminate()
                scheduler_1_process.wait()

                scheduler_1_status = scheduler_1_process.poll()
                assert scheduler_1_status is not None, (
                    "The scheduler_1 process status is None, which means that it hasn't terminated as expected."
                )

            if apiserver_process is not None:
                apiserver_process.terminate()
                apiserver_process.wait()

                apiserver_status = apiserver_process.poll()
                assert apiserver_status is not None, (
                    "The apiserver process status is None, which means that it hasn't terminated as expected."
                )

        out, err = capfd.readouterr()
        log.info("out-start --\n%s\n-- out-end", out)
        log.info("err-start --\n%s\n-- err-end", err)

    @pytest.mark.parametrize(
        "flag_enabled", [pytest.param("True", id="with_fts"), pytest.param("False", id="fts_disabled")]
    )
    def test_topologies(self, flag_enabled: str, monkeypatch, celery_worker_env_vars, capfd, session):
        os.environ["AIRFLOW__SCHEDULER__ENABLE_FAIR_TASK_SELECTION"] = flag_enabled

        scheduler_1_process = None

        celery_worker_1_process = None
        celery_worker_2_process = None
        celery_worker_3_process = None

        apiserver_process = None

        branching_dag_id = "branching_dag"
        branching_dag_2_id = "branching_dag_2"
        branching_dag_3_id = "branching_dag_3"
        branching_dag_4_id = "branching_dag_4"
        branching_dag_5_id = "branching_dag_5"
        linear_dag_id = "linear_dag"
        linear_dag_2_id = "linear_dag_2"
        linear_dag_3_id = "linear_dag_3"
        linear_dag_4_id = "linear_dag_4"
        linear_dag_5_id = "linear_dag_5"
        single_root_with_parallels_id = "single_root_with_parallels"
        single_root_with_parallels_2_id = "single_root_with_parallels_2"

        branching_dag_run_id = None
        branching_dag_2_run_id = None
        branching_dag_3_run_id = None
        branching_dag_4_run_id = None
        branching_dag_5_run_id = None
        linear_dag_run_id = None
        linear_dag_2_run_id = None
        linear_dag_3_run_id = None
        linear_dag_4_run_id = None
        linear_dag_5_run_id = None
        single_root_with_parallels_run_id = None
        single_root_with_parallels_2_run_id = None

        try:
            # Start the processes here and not as fixtures or in a common setup,
            # so that the test can capture their output.
            (
                scheduler_1_process,
                celery_worker_1_process,
                celery_worker_2_process,
                celery_worker_3_process,
                apiserver_process,
            ) = self.start_schedulers_and_workers(second_sched=False)

            assert len(self.dags) > 0
            branching_dag = self.dags[branching_dag_id]
            branching_dag_2 = self.dags[branching_dag_2_id]
            branching_dag_3 = self.dags[branching_dag_3_id]
            branching_dag_4 = self.dags[branching_dag_4_id]
            branching_dag_5 = self.dags[branching_dag_5_id]
            linear_dag = self.dags[linear_dag_id]
            linear_dag_2 = self.dags[linear_dag_2_id]
            linear_dag_3 = self.dags[linear_dag_3_id]
            linear_dag_4 = self.dags[linear_dag_4_id]
            linear_dag_5 = self.dags[linear_dag_5_id]
            single_root_with_parallels = self.dags[single_root_with_parallels_id]
            single_root_with_parallels_2 = self.dags[single_root_with_parallels_2_id]

            assert branching_dag is not None
            assert branching_dag_2 is not None
            assert branching_dag_3 is not None
            assert branching_dag_4 is not None
            assert branching_dag_5 is not None
            assert linear_dag is not None
            assert linear_dag_2 is not None
            assert linear_dag_3 is not None
            assert linear_dag_4 is not None
            assert linear_dag_5 is not None
            assert single_root_with_parallels is not None
            assert single_root_with_parallels_2 is not None

            # 4 dag_runs
            branching_dag_run_id = unpause_trigger_dag_and_get_run_id(dag_id=branching_dag_id)
            branching_dag_run_id2 = unpause_trigger_dag_and_get_run_id(dag_id=branching_dag_id, unpause=False)
            branching_dag_run_id3 = unpause_trigger_dag_and_get_run_id(dag_id=branching_dag_id, unpause=False)
            branching_dag_run_id4 = unpause_trigger_dag_and_get_run_id(dag_id=branching_dag_id, unpause=False)

            branching_dag_2_run_id = unpause_trigger_dag_and_get_run_id(dag_id=branching_dag_2_id)

            # 3 dag_runs
            branching_dag_3_run_id = unpause_trigger_dag_and_get_run_id(dag_id=branching_dag_3_id)
            branching_dag_3_run_id2 = unpause_trigger_dag_and_get_run_id(
                dag_id=branching_dag_3_id, unpause=False
            )
            branching_dag_3_run_id3 = unpause_trigger_dag_and_get_run_id(
                dag_id=branching_dag_3_id, unpause=False
            )

            branching_dag_4_run_id = unpause_trigger_dag_and_get_run_id(dag_id=branching_dag_4_id)
            branching_dag_5_run_id = unpause_trigger_dag_and_get_run_id(dag_id=branching_dag_5_id)

            linear_dag_run_id = unpause_trigger_dag_and_get_run_id(dag_id=linear_dag_id)

            # 4 dag_runs
            linear_dag_2_run_id = unpause_trigger_dag_and_get_run_id(dag_id=linear_dag_2_id)
            linear_dag_2_run_id2 = unpause_trigger_dag_and_get_run_id(dag_id=linear_dag_2_id, unpause=False)
            linear_dag_2_run_id3 = unpause_trigger_dag_and_get_run_id(dag_id=linear_dag_2_id, unpause=False)
            linear_dag_2_run_id4 = unpause_trigger_dag_and_get_run_id(dag_id=linear_dag_2_id, unpause=False)

            linear_dag_3_run_id = unpause_trigger_dag_and_get_run_id(dag_id=linear_dag_3_id)

            # 2 dag_runs
            linear_dag_4_run_id = unpause_trigger_dag_and_get_run_id(dag_id=linear_dag_4_id)
            linear_dag_4_run_id2 = unpause_trigger_dag_and_get_run_id(dag_id=linear_dag_4_id, unpause=False)

            linear_dag_5_run_id = unpause_trigger_dag_and_get_run_id(dag_id=linear_dag_5_id)
            single_root_with_parallels_run_id = unpause_trigger_dag_and_get_run_id(
                dag_id=single_root_with_parallels_id
            )

            # 3 dag_runs
            single_root_with_parallels_2_run_id = unpause_trigger_dag_and_get_run_id(
                dag_id=single_root_with_parallels_2_id
            )
            single_root_with_parallels_2_run_id2 = unpause_trigger_dag_and_get_run_id(
                dag_id=single_root_with_parallels_2_id, unpause=False
            )
            single_root_with_parallels_2_run_id3 = unpause_trigger_dag_and_get_run_id(
                dag_id=single_root_with_parallels_2_id, unpause=False
            )

            # 4 DRs for branching_dag_id
            wait_for_dag_run(dag_id=branching_dag_id, run_id=branching_dag_run_id, max_wait_time=9000)
            wait_for_dag_run(dag_id=branching_dag_id, run_id=branching_dag_run_id2, max_wait_time=9000)
            wait_for_dag_run(dag_id=branching_dag_id, run_id=branching_dag_run_id3, max_wait_time=9000)
            wait_for_dag_run(dag_id=branching_dag_id, run_id=branching_dag_run_id4, max_wait_time=9000)

            wait_for_dag_run(dag_id=branching_dag_2_id, run_id=branching_dag_2_run_id, max_wait_time=9000)

            # 3 DRs for branching_dag_3_id
            wait_for_dag_run(dag_id=branching_dag_3_id, run_id=branching_dag_3_run_id, max_wait_time=9000)
            wait_for_dag_run(dag_id=branching_dag_3_id, run_id=branching_dag_3_run_id2, max_wait_time=9000)
            wait_for_dag_run(dag_id=branching_dag_3_id, run_id=branching_dag_3_run_id3, max_wait_time=9000)

            wait_for_dag_run(dag_id=branching_dag_4_id, run_id=branching_dag_4_run_id, max_wait_time=9000)
            wait_for_dag_run(dag_id=branching_dag_5_id, run_id=branching_dag_5_run_id, max_wait_time=9000)

            wait_for_dag_run(dag_id=linear_dag_id, run_id=linear_dag_run_id, max_wait_time=9000)

            # 4 DRs for linear_dag_2_id
            wait_for_dag_run(dag_id=linear_dag_2_id, run_id=linear_dag_2_run_id, max_wait_time=9000)
            wait_for_dag_run(dag_id=linear_dag_2_id, run_id=linear_dag_2_run_id2, max_wait_time=9000)
            wait_for_dag_run(dag_id=linear_dag_2_id, run_id=linear_dag_2_run_id3, max_wait_time=9000)
            wait_for_dag_run(dag_id=linear_dag_2_id, run_id=linear_dag_2_run_id4, max_wait_time=9000)

            wait_for_dag_run(dag_id=linear_dag_3_id, run_id=linear_dag_3_run_id, max_wait_time=9000)

            # 2 DRs for linear_dag_4_id
            wait_for_dag_run(dag_id=linear_dag_4_id, run_id=linear_dag_4_run_id, max_wait_time=9000)
            wait_for_dag_run(dag_id=linear_dag_4_id, run_id=linear_dag_4_run_id2, max_wait_time=9000)

            wait_for_dag_run(dag_id=linear_dag_5_id, run_id=linear_dag_5_run_id, max_wait_time=9000)

            wait_for_dag_run(
                dag_id=single_root_with_parallels_id,
                run_id=single_root_with_parallels_run_id,
                max_wait_time=9000,
            )

            # 3 DRs for single_root_with_parallels_2_id
            wait_for_dag_run(
                dag_id=single_root_with_parallels_2_id,
                run_id=single_root_with_parallels_2_run_id,
                max_wait_time=9000,
            )
            wait_for_dag_run(
                dag_id=single_root_with_parallels_2_id,
                run_id=single_root_with_parallels_2_run_id2,
                max_wait_time=9000,
            )
            wait_for_dag_run(
                dag_id=single_root_with_parallels_2_id,
                run_id=single_root_with_parallels_2_run_id3,
                max_wait_time=9000,
            )

            time.sleep(10)
        finally:
            if branching_dag_run_id is not None:
                print_ti_output_for_dag_run(dag_id=branching_dag_id, run_id=branching_dag_run_id)
            if branching_dag_2_id is not None:
                print_ti_output_for_dag_run(dag_id=branching_dag_2_id, run_id=branching_dag_2_run_id)
            if branching_dag_3_id is not None:
                print_ti_output_for_dag_run(dag_id=branching_dag_3_id, run_id=branching_dag_3_run_id)
            if branching_dag_4_id is not None:
                print_ti_output_for_dag_run(dag_id=branching_dag_4_id, run_id=branching_dag_4_run_id)
            if branching_dag_5_id is not None:
                print_ti_output_for_dag_run(dag_id=branching_dag_5_id, run_id=branching_dag_5_run_id)

            if linear_dag_run_id is not None:
                print_ti_output_for_dag_run(dag_id=linear_dag_id, run_id=linear_dag_run_id)
            if linear_dag_2_run_id is not None:
                print_ti_output_for_dag_run(dag_id=linear_dag_2_id, run_id=linear_dag_2_run_id)
            if linear_dag_3_run_id is not None:
                print_ti_output_for_dag_run(dag_id=linear_dag_3_id, run_id=linear_dag_3_run_id)
            if linear_dag_4_run_id is not None:
                print_ti_output_for_dag_run(dag_id=linear_dag_4_id, run_id=linear_dag_4_run_id)
            if linear_dag_5_run_id is not None:
                print_ti_output_for_dag_run(dag_id=linear_dag_5_id, run_id=linear_dag_5_run_id)

            if single_root_with_parallels_run_id is not None:
                print_ti_output_for_dag_run(
                    dag_id=single_root_with_parallels_id,
                    run_id=single_root_with_parallels_run_id,
                )
            if single_root_with_parallels_2_run_id is not None:
                print_ti_output_for_dag_run(
                    dag_id=single_root_with_parallels_2_id,
                    run_id=single_root_with_parallels_2_run_id,
                )

            # Terminate the processes.
            if celery_worker_1_process is not None:
                celery_worker_1_process.terminate()
                celery_worker_1_process.wait()

                celery_1_status = celery_worker_1_process.poll()
                assert celery_1_status is not None, (
                    "The celery_worker_1 process status is None, which means that it hasn't terminated as expected."
                )

            if celery_worker_2_process is not None:
                celery_worker_2_process.terminate()
                celery_worker_2_process.wait()

                celery_2_status = celery_worker_2_process.poll()
                assert celery_2_status is not None, (
                    "The celery_worker_2 process status is None, which means that it hasn't terminated as expected."
                )

            if celery_worker_3_process is not None:
                celery_worker_3_process.terminate()
                celery_worker_3_process.wait()

                celery_3_status = celery_worker_3_process.poll()
                assert celery_3_status is not None, (
                    "The celery_worker_3 process status is None, which means that it hasn't terminated as expected."
                )

            if scheduler_1_process is not None:
                scheduler_1_process.terminate()
                scheduler_1_process.wait()

                scheduler_1_status = scheduler_1_process.poll()
                assert scheduler_1_status is not None, (
                    "The scheduler_1 process status is None, which means that it hasn't terminated as expected."
                )

            if apiserver_process is not None:
                apiserver_process.terminate()
                apiserver_process.wait()

                apiserver_status = apiserver_process.poll()
                assert apiserver_status is not None, (
                    "The apiserver process status is None, which means that it hasn't terminated as expected."
                )

        out, err = capfd.readouterr()
        log.info("out-start --\n%s\n-- out-end", out)
        log.info("err-start --\n%s\n-- err-end", err)

    @pytest.mark.parametrize(
        "flag_enabled", [pytest.param("True", id="with_fts"), pytest.param("False", id="fts_disabled")]
    )
    def test_heavy_load(self, flag_enabled: str, monkeypatch, celery_worker_env_vars, capfd, session):
        os.environ["AIRFLOW__SCHEDULER__ENABLE_FAIR_TASK_SELECTION"] = flag_enabled

        scheduler_1_process = None

        celery_worker_1_process = None
        celery_worker_2_process = None
        celery_worker_3_process = None

        apiserver_process = None

        dag_45_id = "dag_45_tasks"
        dag_250_id = "dag_250_tasks"
        dag_470_id = "dag_470_tasks"
        dag_1000_id = "dag_1000_tasks"
        dag_1100_id = "dag_1100_tasks"
        dag_1200_id = "dag_1200_tasks"

        dag_45_run_id = None
        dag_250_run_id = None
        dag_470_run_id = None
        dag_1000_run_id = None
        dag_1100_run_id = None
        dag_1200_run_id = None

        try:
            # Start the processes here and not as fixtures or in a common setup,
            # so that the test can capture their output.
            (
                scheduler_1_process,
                celery_worker_1_process,
                celery_worker_2_process,
                celery_worker_3_process,
                apiserver_process,
            ) = self.start_schedulers_and_workers(second_sched=False)

            assert len(self.dags) > 0
            dag_45 = self.dags[dag_45_id]
            dag_250 = self.dags[dag_250_id]
            dag_470 = self.dags[dag_470_id]
            dag_1000 = self.dags[dag_1000_id]
            dag_1100 = self.dags[dag_1100_id]
            dag_1200 = self.dags[dag_1200_id]

            assert dag_45 is not None
            assert dag_250 is not None
            assert dag_470 is not None
            assert dag_1000 is not None
            assert dag_1100 is not None
            assert dag_1200 is not None

            # --- after start_scheduler_and_workers() ----------------

            dag_1200_run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_1200_id)
            dag_470_run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_470_id)
            dag_1000_run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_1000_id)
            dag_1100_run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_1100_id)
            dag_250_run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_250_id)
            dag_45_run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_45_id)

            wait_for_dag_run(dag_id=dag_45_id, run_id=dag_45_run_id, max_wait_time=9000)

            wait_for_dag_run(dag_id=dag_250_id, run_id=dag_250_run_id, max_wait_time=9000)

            wait_for_dag_run(dag_id=dag_470_id, run_id=dag_470_run_id, max_wait_time=9000)

            wait_for_dag_run(dag_id=dag_1000_id, run_id=dag_1000_run_id, max_wait_time=9000)

            wait_for_dag_run(dag_id=dag_1100_id, run_id=dag_1100_run_id, max_wait_time=9000)

            wait_for_dag_run(dag_id=dag_1200_id, run_id=dag_1200_run_id, max_wait_time=9000)

            time.sleep(10)
        finally:
            if dag_45_run_id is not None:
                print_ti_output_for_dag_run(dag_id=dag_45_id, run_id=dag_45_run_id)
            if dag_250_run_id is not None:
                print_ti_output_for_dag_run(dag_id=dag_250_id, run_id=dag_250_run_id)
            if dag_470_run_id is not None:
                print_ti_output_for_dag_run(dag_id=dag_470_id, run_id=dag_470_run_id)
            if dag_1000_run_id is not None:
                print_ti_output_for_dag_run(dag_id=dag_1000_id, run_id=dag_1000_run_id)
            if dag_1100_run_id is not None:
                print_ti_output_for_dag_run(dag_id=dag_1100_id, run_id=dag_1100_run_id)
            if dag_1200_run_id is not None:
                print_ti_output_for_dag_run(dag_id=dag_1200_id, run_id=dag_1200_run_id)

            # Terminate the processes.
            if celery_worker_1_process is not None:
                celery_worker_1_process.terminate()
                celery_worker_1_process.wait()

                celery_1_status = celery_worker_1_process.poll()
                assert celery_1_status is not None, (
                    "The celery_worker_1 process status is None, which means that it hasn't terminated as expected."
                )

            if celery_worker_2_process is not None:
                celery_worker_2_process.terminate()
                celery_worker_2_process.wait()

                celery_2_status = celery_worker_2_process.poll()
                assert celery_2_status is not None, (
                    "The celery_worker_2 process status is None, which means that it hasn't terminated as expected."
                )

            if celery_worker_3_process is not None:
                celery_worker_3_process.terminate()
                celery_worker_3_process.wait()

                celery_3_status = celery_worker_3_process.poll()
                assert celery_3_status is not None, (
                    "The celery_worker_3 process status is None, which means that it hasn't terminated as expected."
                )

            if scheduler_1_process is not None:
                scheduler_1_process.terminate()
                scheduler_1_process.wait()

                scheduler_1_status = scheduler_1_process.poll()
                assert scheduler_1_status is not None, (
                    "The scheduler_1 process status is None, which means that it hasn't terminated as expected."
                )

            if apiserver_process is not None:
                apiserver_process.terminate()
                apiserver_process.wait()

                apiserver_status = apiserver_process.poll()
                assert apiserver_status is not None, (
                    "The apiserver process status is None, which means that it hasn't terminated as expected."
                )

        out, err = capfd.readouterr()
        log.info("out-start --\n%s\n-- out-end", out)
        log.info("err-start --\n%s\n-- err-end", err)

    def start_schedulers_and_workers(self, second_sched: bool):
        scheduler1 = subprocess.Popen(
            self.scheduler_command_args,
            env=os.environ.copy(),
            stdout=None,
            stderr=None,
        )

        scheduler2 = None
        if second_sched:
            scheduler2 = subprocess.Popen(
                self.scheduler_command_args,
                env=os.environ.copy(),
                stdout=None,
                stderr=None,
            )

        worker1_args = [*self.celery_command_args, "--hostname", "worker1"]
        worker2_args = [*self.celery_command_args, "--hostname", "worker2"]
        worker3_args = [*self.celery_command_args, "--hostname", "worker3"]

        worker1 = subprocess.Popen(
            worker1_args,
            env=os.environ.copy(),
            stdout=None,
            stderr=None,
        )

        worker2 = subprocess.Popen(
            worker2_args,
            env=os.environ.copy(),
            stdout=None,
            stderr=None,
        )

        worker3 = subprocess.Popen(
            worker3_args,
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

        # Sanity checks. Fail fast if error.
        for name, p in [
            ("scheduler1", scheduler1),
            ("worker1", worker1),
            ("worker2", worker2),
            ("worker3", worker3),
        ]:
            if p.poll() is not None:
                raise RuntimeError(f"{name} exited early with code {p.returncode}")

        if second_sched:
            return scheduler1, scheduler2, worker1, worker2, worker3, apiserver_process
        return scheduler1, worker1, worker2, worker3, apiserver_process
