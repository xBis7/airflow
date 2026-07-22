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
import socket
import subprocess
import time

import pytest
import requests
from sqlalchemy import func, select

from airflow._shared.timezones import timezone
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.dagbag import DagBag
from airflow.executors import executor_loader
from airflow.executors.executor_utils import ExecutorName
from airflow.models import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.serialization.definitions.dag import SerializedDAG
from airflow.utils.session import create_session
from airflow.utils.state import State

from tests_common.test_utils.dag import create_scheduler_dag
from tests_common.test_utils.otel_jaeger_utils import (
    get_span_tags,
    provided_child_spans_found_under_span,
)
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS

log = logging.getLogger("integration.perf.test_perf")


def wait_for_otel_collector(host: str, port: int, timeout: int = 120) -> None:
    """
    Wait for the OTel collector to be reachable before running tests.

    This prevents flaky test failures caused by transient DNS resolution issues
    (e.g., 'Temporary failure in name resolution' for breeze-otel-collector).

    Note: If the collector is not reachable after timeout, logs a warning but
    does not fail - allows tests to run and fail naturally if needed.
    """
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            # Test DNS resolution and TCP connectivity
            with socket.create_connection((host, port), timeout=5):
                pass
            log.info("OTel collector at %s:%d is reachable.", host, port)
            return
        except (socket.gaierror, TimeoutError, OSError) as e:
            last_error = e
            log.debug(
                "OTel collector at %s:%d not reachable: %s. Retrying...",
                host,
                port,
                e,
            )
            time.sleep(2)
    log.warning(
        "OTel collector at %s:%d is not reachable after %ds. Last error: %s. "
        "Tests will proceed but may fail if collector is required.",
        host,
        port,
        timeout,
        last_error,
    )


def unpause_trigger_dag_and_get_run_id(dag_id: str, unpause: bool = True, conf: dict | None = None) -> str:
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

    if conf:
        import json

        trigger_command += ["--conf", json.dumps(conf)]

    # Trigger the dag using the cli.
    subprocess.run(trigger_command, check=True, env=os.environ.copy())

    return run_id


def wait_for_dag_run(dag_id: str, run_id: str, max_wait_time: int):
    # max_wait_time, is the timeout for the DAG run to complete. The value is in seconds.
    start_time = timezone.utcnow().timestamp()

    while timezone.utcnow().timestamp() - start_time < max_wait_time:
        with create_session() as session:
            dag_run = session.scalar(
                select(DagRun).where(
                    DagRun.dag_id == dag_id,
                    DagRun.run_id == run_id,
                )
            )

            if dag_run is None:
                time.sleep(5)
                continue

            dag_run_state = dag_run.state
            log.debug("DAG Run state: %s.", dag_run_state)

            if dag_run_state in [State.SUCCESS, State.FAILED]:
                break
    return dag_run_state


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
                print("\n===== LOG FILE: %s - START =====\n", full_path)
                try:
                    with open(full_path) as f:
                        print(f.read())
                except Exception as e:
                    log.error("Could not read %s: %s", full_path, e)

                print("\n===== END =====\n")


def get_non_idle_loop_trace(all_traces: list[dict]) -> dict | None:
    for trace in all_traces:
        scheduler_loop_span = next(
            (s for s in trace["spans"] if s["operationName"] == "scheduler.scheduler_loop"),
            None,
        )
        if scheduler_loop_span is None:
            continue
        # A non-idle scheduler_loop span means this iteration actually scheduled something.
        if get_span_tags(scheduler_loop_span).get("airflow.scheduler.loop_iteration.idle") is False:
            return trace
    return None


# Resource profiles selected by `TestPerformanceIntegration.machine_profile`.
# The macbook profile keeps Docker container resource usage low enough that the
# containers don't get force-killed on a laptop. The ubuntu profile is tuned for a
# more powerful desktop where the 3 Celery workers give 3 * 15 = 45 execution slots,
# so parallelism and the default pool are sized above that to keep workers saturated
# and push a real backlog of QUEUED tasks (and broker publishes) through the scheduler.
# Concurrency is deliberately kept below the Postgres max_connections ceiling: each of
# the fixed set of DB-connected processes (scheduler, api-server workers, celery workers,
# result backend) holds its own SQLAlchemy pool, and running-task concurrency drives how
# many api-server sessions open at once. Too many slots exhausts connections and the
# scheduler dies with "sorry, too many clients already".
RESOURCE_PROFILES = {
    "macbook": {
        "worker_concurrency": "5",
        "max_tis_per_query": "100",
        "parallelism": "100",
        "max_active_tasks_per_dag": "8",
        "max_active_runs_per_dag": "10",
        "default_pool_task_slot_count": "64",
    },
    "ubuntu": {
        "worker_concurrency": "15",
        "max_tis_per_query": "256",
        "parallelism": "256",
        "max_active_tasks_per_dag": "64",
        "max_active_runs_per_dag": "10",
        "default_pool_task_slot_count": "256",
    },
}


@pytest.mark.integration("otel")
@pytest.mark.integration("redis")
@pytest.mark.backend("postgres")
class TestPerformanceIntegration:
    test_dir = os.path.dirname(os.path.abspath(__file__))
    # TODO: adjust the last folder to avoid loading everything. Or remove it.
    dag_folder = os.path.join(test_dir, "dags")

    dag_num = os.getenv("dag_num", default="2")
    log_level = os.getenv("log_level", default="none")

    # Manually edit to select the resource profile for the machine running the test:
    # "macbook" for the laptop, "ubuntu" for the more powerful desktop. See RESOURCE_PROFILES.
    machine_profile = "macbook"

    celery_command_args = [
        "celery",
        "--app",
        "airflow.providers.celery.executors.celery_executor.app",
        "worker",
        "--concurrency",
        RESOURCE_PROFILES[machine_profile]["worker_concurrency"],
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

    dags: dict[str, SerializedDAG] = {}

    @classmethod
    def setup_class(cls):
        otel_host = "breeze-otel-collector"
        otel_port = 4318

        # Wait for OTel collector to be reachable before running tests.
        # This prevents flaky test failures caused by transient DNS resolution issues
        # during scheduler handoff (see https://github.com/apache/airflow/issues/61070).
        wait_for_otel_collector(otel_host, otel_port)

        # The pytest plugin strips AIRFLOW__*__* env vars (including the JWT secret set
        # by Breeze). Both the scheduler and api-server subprocesses must share the same
        # secret; otherwise each generates its own random key and token verification fails.
        os.environ["AIRFLOW__API_AUTH__JWT_SECRET"] = "test-secret-key-for-testing"
        os.environ["AIRFLOW__API_AUTH__JWT_ISSUER"] = "airflow"
        os.environ["AIRFLOW__TRACES__OTEL_ON"] = "True"
        os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "http/protobuf"
        os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = "http://breeze-otel-collector:4318/v1/traces"

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

        profile = RESOURCE_PROFILES[cls.machine_profile]

        # How many the scheduler can schedule at once. Sized to parallelism so the
        # critical-section SELECT/queue batch is the full slot budget on a busy loop.
        os.environ["AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY"] = profile["max_tis_per_query"]
        os.environ["AIRFLOW__SCHEDULER__MAX_DAGRUNS_TO_CREATE_PER_LOOP"] = "10"
        os.environ["AIRFLOW__SCHEDULER__MAX_DAGRUNS_PER_LOOP_TO_SCHEDULE"] = "20"
        os.environ["AIRFLOW__SCHEDULER__PARSING_PROCESSES"] = "2"

        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{cls.dag_folder}"

        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = "/dev/null"
        os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"

        # Concurrency budget (machine-dependent, see RESOURCE_PROFILES). parallelism and the
        # default pool are sized against the worker execution slots to keep workers saturated and
        # push a real backlog of QUEUED tasks (and broker publishes) through the scheduler.
        # The load dags are "wide" (hundreds of independent tasks each), so max_active_tasks_per_dag
        # is the real throttle — sized so several dags can share the pool concurrently.
        os.environ["AIRFLOW__CORE__PARALLELISM"] = profile["parallelism"]
        # Number of tasks that can run concurrently per dag.
        os.environ["AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG"] = profile["max_active_tasks_per_dag"]
        # Number of active dag_runs per dag.
        os.environ["AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG"] = profile["max_active_runs_per_dag"]
        # Set before `db migrate` below so the default pool row is created with this many slots.
        os.environ["AIRFLOW__CORE__DEFAULT_POOL_TASK_SLOT_COUNT"] = profile["default_pool_task_slot_count"]

        # metrics
        os.environ["AIRFLOW__METRICS__OTEL_ON"] = "True"
        os.environ["AIRFLOW__METRICS__OTEL_HOST"] = "breeze-otel-collector"
        os.environ["AIRFLOW__METRICS__OTEL_PORT"] = "4318"
        os.environ["AIRFLOW__METRICS__OTEL_INTERVAL_MILLISECONDS"] = "1000"

        # traces
        os.environ["AIRFLOW__TRACES__OTEL_ON"] = "True"
        os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "http/protobuf"
        os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = "http://breeze-otel-collector:4318/v1/traces"

        # os.environ["AIRFLOW__CELERY__WORKER_CONCURRENCY"] = "100"
        os.environ["AIRFLOW__CELERY__EXTRA_CELERY_CONFIG"] = '{"worker_max_tasks_per_child": 100}'
        os.environ["AIRFLOW__CELERY__WORKER_PREFETCH_MULTIPLIER"] = "1"
        # SYNC_PARALLELISM=1 keeps _send_workloads_to_celery on the single-thread
        # sequential branch. The default (0 → cpu_count) triggers the process_pool
        # branch for any batch of >=2 workloads, spawning a ProcessPoolExecutor per
        # publish call — hundreds of ms of fork/shutdown overhead on the sync
        # scheduler heartbeat. Redis publish is I/O-bound, so process-level
        # parallelism gives ~nothing anyway.
        os.environ["AIRFLOW__CELERY__SYNC_PARALLELISM"] = "1"
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
    def serialize_and_get_dags(cls) -> dict[str, SerializedDAG]:
        log.info("Serializing Dags from directory %s", cls.dag_folder)
        # Load DAGs from the dag directory.
        dag_bag = DagBag(dag_folder=cls.dag_folder)

        dag_ids = dag_bag.dag_ids
        assert len(dag_ids) > 2

        dag_dict: dict[str, SerializedDAG] = {}
        with create_session() as session:
            for dag_id in dag_ids:
                dag = dag_bag.get_dag(dag_id)
                assert dag is not None, f"DAG with ID {dag_id} not found."
                # Sync the DAG to the database.
                if AIRFLOW_V_3_0_PLUS:
                    from airflow.models.dagbundle import DagBundleModel

                    count = session.scalar(
                        select(func.count())
                        .select_from(DagBundleModel)
                        .where(DagBundleModel.name == "testing")
                    )
                    if count == 0:
                        session.add(DagBundleModel(name="testing"))
                        session.commit()
                    SerializedDAG.bulk_write_to_db(
                        bundle_name="testing", bundle_version=None, dags=[dag], session=session
                    )
                    dag_dict[dag_id] = create_scheduler_dag(dag)
                else:
                    dag.sync_to_db(session=session)
                    dag_dict[dag_id] = dag
                # Manually serialize the dag and write it to the db to avoid a db error.
                if AIRFLOW_V_3_1_PLUS:
                    from airflow.serialization.serialized_objects import LazyDeserializedDAG

                    SerializedDagModel.write_dag(
                        LazyDeserializedDAG.from_dag(dag), bundle_name="testing", session=session
                    )
                else:
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
        monkeypatch.setattr(
            executor_loader, "_alias_to_executors_per_team", {"CeleryExecutor": executor_name}
        )

    @pytest.mark.execution_timeout(160)
    def test_scheduler_debug_traces(self, monkeypatch, celery_worker_env_vars, capfd, session):
        # Enable debug traces.
        os.environ["AIRFLOW__TRACES__OTEL_DEBUG_TRACES_ON"] = "True"

        processes: dict[str, subprocess.Popen] = {}
        try:
            processes = self.start_schedulers_and_workers(num_workers=1)

            dag_id = "demo_dag"

            assert len(self.dags) > 0
            dag = self.dags[dag_id]

            assert dag is not None

            conf = None

            run_id_1 = unpause_trigger_dag_and_get_run_id(dag_id=dag_id, conf=conf)
            run_id_2 = unpause_trigger_dag_and_get_run_id(dag_id=dag_id, conf=conf)

            wait_for_dag_run(dag_id=dag_id, run_id=run_id_1, max_wait_time=90)
            wait_for_dag_run(dag_id=dag_id, run_id=run_id_2, max_wait_time=90)

            time.sleep(10)

            print_ti_output_for_dag_run(dag_id=dag_id, run_id=run_id_1)
            print_ti_output_for_dag_run(dag_id=dag_id, run_id=run_id_2)
        finally:
            # Terminate the processes.
            self._terminate_all(processes)

        capfd.readouterr()

        host = "jaeger"
        service_name = os.environ.get("OTEL_SERVICE_NAME", "test")
        r = requests.get(f"http://{host}:16686/api/traces?service={service_name}")
        traces = r.json()["data"]

        # There are spans that don't get exported on every loop iteration. The ones below
        # reliably re-appear every time that the loop does some work.
        expected_children_spans = [
            "scheduler._do_scheduling",
            "scheduler.critical_section",
            "scheduler._process_executor_events",
        ]

        non_idle_trace = get_non_idle_loop_trace(traces)
        assert non_idle_trace is not None, "expected at least one non-idle scheduler_loop span"

        assert provided_child_spans_found_under_span(
            non_idle_trace, "scheduler.scheduler_loop", expected_children_spans
        ), f"expected {expected_children_spans} as descendants of scheduler.scheduler_loop"

    def test_topologies(self, monkeypatch, celery_worker_env_vars, capfd, session):
        # Enable debug traces.
        os.environ["AIRFLOW__TRACES__OTEL_DEBUG_TRACES_ON"] = "True"

        processes: dict[str, subprocess.Popen] = {}

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
            processes = self.start_schedulers_and_workers(num_workers=3)

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
            self._terminate_all(processes)

        out, err = capfd.readouterr()
        log.info("out-start --\n%s\n-- out-end", out)
        log.info("err-start --\n%s\n-- err-end", err)

    def test_heavy_load(self, monkeypatch, celery_worker_env_vars, capfd, session):
        # Enable debug traces.
        os.environ["AIRFLOW__TRACES__OTEL_DEBUG_TRACES_ON"] = "True"

        processes: dict[str, subprocess.Popen] = {}

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
            processes = self.start_schedulers_and_workers(num_workers=3)

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
            # dag_470_run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_470_id)
            # dag_1000_run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_1000_id)
            # dag_1100_run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_1100_id)
            # dag_250_run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_250_id)
            dag_45_run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_45_id)

            wait_for_dag_run(dag_id=dag_45_id, run_id=dag_45_run_id, max_wait_time=9000)

            # wait_for_dag_run(dag_id=dag_250_id, run_id=dag_250_run_id, max_wait_time=9000)

            # wait_for_dag_run(dag_id=dag_470_id, run_id=dag_470_run_id, max_wait_time=9000)

            # wait_for_dag_run(dag_id=dag_1000_id, run_id=dag_1000_run_id, max_wait_time=9000)

            # wait_for_dag_run(dag_id=dag_1100_id, run_id=dag_1100_run_id, max_wait_time=9000)

            wait_for_dag_run(dag_id=dag_1200_id, run_id=dag_1200_run_id, max_wait_time=9000)

            time.sleep(10)
        finally:
            if dag_45_run_id is not None:
                print_ti_output_for_dag_run(dag_id=dag_45_id, run_id=dag_45_run_id)
            # if dag_250_run_id is not None:
            #     print_ti_output_for_dag_run(dag_id=dag_250_id, run_id=dag_250_run_id)
            # if dag_470_run_id is not None:
            #     print_ti_output_for_dag_run(dag_id=dag_470_id, run_id=dag_470_run_id)
            # if dag_1000_run_id is not None:
            #     print_ti_output_for_dag_run(dag_id=dag_1000_id, run_id=dag_1000_run_id)
            # if dag_1100_run_id is not None:
            #     print_ti_output_for_dag_run(dag_id=dag_1100_id, run_id=dag_1100_run_id)
            if dag_1200_run_id is not None:
                print_ti_output_for_dag_run(dag_id=dag_1200_id, run_id=dag_1200_run_id)

            # Terminate the processes.
            self._terminate_all(processes)

        out, err = capfd.readouterr()
        log.info("out-start --\n%s\n-- out-end", out)
        log.info("err-start --\n%s\n-- err-end", err)

    @staticmethod
    def _terminate_process(proc: subprocess.Popen, timeout: int = 30) -> None:
        # Grace period covers OTel atexit flush (force_flush default: 10s);
        # SIGKILL is the fallback if the process is still alive after timeout.
        proc.terminate()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    def _terminate_all(self, processes: dict[str, subprocess.Popen]) -> None:
        for name, proc in processes.items():
            self._terminate_process(proc)
            assert proc.poll() is not None, (
                f"The {name} process status is None, which means that it hasn't terminated as expected."
            )

    def start_schedulers_and_workers(
        self,
        num_workers: int = 1,
        second_sched: bool = False,
    ) -> dict[str, subprocess.Popen]:
        processes: dict[str, subprocess.Popen] = {}
        try:
            processes["scheduler_1"] = subprocess.Popen(
                self.scheduler_command_args,
                env=os.environ.copy(),
                stdout=None,
                stderr=None,
            )

            if second_sched:
                processes["scheduler_2"] = subprocess.Popen(
                    self.scheduler_command_args,
                    env=os.environ.copy(),
                    stdout=None,
                    stderr=None,
                )

            for i in range(1, num_workers + 1):
                worker_args = [*self.celery_command_args, "--hostname", f"worker{i}"]
                processes[f"worker_{i}"] = subprocess.Popen(
                    worker_args,
                    env=os.environ.copy(),
                    stdout=None,
                    stderr=None,
                )

            processes["apiserver"] = subprocess.Popen(
                self.apiserver_command_args,
                env=os.environ.copy(),
                stdout=None,
                stderr=None,
            )

            # Wait to ensure processes have started.
            time.sleep(10)

            # Sanity checks. Fail fast if error.
            for name, p in processes.items():
                if name == "apiserver":
                    continue
                if p.poll() is not None:
                    raise RuntimeError(f"{name} exited early with code {p.returncode}")
        except Exception:
            # Don't leak already-started processes if any check fails.
            self._terminate_all(processes)
            raise

        return processes
