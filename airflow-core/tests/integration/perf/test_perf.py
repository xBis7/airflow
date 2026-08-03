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

import contextlib
import datetime
import gzip
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import pytest
import requests
from sqlalchemy import func, select, tuple_, update

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.dagbag import DagBag
from airflow.executors import executor_loader
from airflow.executors.executor_utils import ExecutorName
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from airflow.serialization.definitions.dag import SerializedDAG
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.dag import create_scheduler_dag
from tests_common.test_utils.otel_jaeger_utils import (
    get_span_tags,
    provided_child_spans_found_under_span,
)
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS

if TYPE_CHECKING:
    from typing import TextIO

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


def create_queued_dag_runs(dag: SerializedDAG, dag_id: str, count: int) -> list[str]:
    """
    Unpause ``dag_id`` and create ``count`` QUEUED manual dag_runs straight through the ORM.

    The CLI trigger path costs ~10s per invocation, so a loop of them delivers dag_runs at
    ~0.1/s. By Little's Law that caps live dag_runs at ``arrival_rate * duration`` — around 6
    for this workload — which throttles the scheduler far below saturation and makes the
    measurement reflect the harness rather than the scheduler. Creating every row up front,
    before any scheduler process exists, removes arrival rate as a variable.

    ``run_after`` is identical for every run so all of them are eligible immediately;
    staggering it would reintroduce the same ramp this function exists to remove.
    """
    base = timezone.utcnow()
    run_ids: list[str] = []
    with create_session() as session:
        session.execute(update(DagModel).where(DagModel.dag_id == dag_id).values(is_paused=False))
        for i in range(count):
            logical_date = base + datetime.timedelta(seconds=i)
            run_id = f"manual__{logical_date.isoformat()}"
            dag.create_dagrun(
                run_id=run_id,
                logical_date=logical_date,
                data_interval=None,
                run_after=base,
                run_type=DagRunType.MANUAL,
                triggered_by=DagRunTriggeredByType.CLI,
                state=DagRunState.QUEUED,
                session=session,
            )
            run_ids.append(run_id)
        session.commit()

    log.info("Created %d queued dag_runs for %s", len(run_ids), dag_id)
    return run_ids


def restore_db_dump(dump_path: str) -> None:
    """
    Replace the metadata DB contents with a gzipped plain-format pg_dump snapshot.

    Plain SQL restored through psql, not a custom-format archive through pg_restore: the
    breeze container's pg_restore (18.x) injects ``SET`` statements the PG 14 server
    rejects, whereas a plain dump contains only SQL the dumping server itself generated.
    The embedded ``DROP TABLE`` statements block on any live connection, so dispose this
    process's own engine pool first, then terminate every other backend on the database.
    """
    settings.get_engine().dispose()
    env = {**os.environ, "PGPASSWORD": "airflow"}
    subprocess.run(
        [
            "psql",
            *("-h", "postgres", "-U", "postgres", "-d", "postgres"),
            "-qAtc",
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
            " WHERE datname='airflow' AND pid <> pg_backend_pid()",
        ],
        check=True,
        env=env,
        capture_output=True,
    )
    with gzip.open(dump_path, "rb") as f:
        sql = f.read()
    subprocess.run(
        ["psql", *("-h", "postgres", "-U", "postgres", "-d", "airflow"), "-q", "-v", "ON_ERROR_STOP=1"],
        check=True,
        env=env,
        input=sql,
        capture_output=True,
    )
    # Freshly restored tables have no planner statistics, and autoanalyze keeps getting
    # canceled by the scheduler's FOR UPDATE locks once the run starts -- the first seeded
    # run measured planner warmup (2.9 -> 14.4 tasks/s over 22 min) instead of the scheduler.
    subprocess.run(
        ["psql", *("-h", "postgres", "-U", "postgres", "-d", "airflow"), "-qc", "ANALYZE"],
        check=True,
        env=env,
        capture_output=True,
    )
    log.info("Restored and analyzed metadata DB from %s", dump_path)


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

        # Without this the loop spins as fast as Postgres can answer, burning a core in
        # the test process and adding query load to the database under measurement.
        time.sleep(2)
    return dag_run_state


def wait_for_dag_runs(run_ids: dict[str, str], max_wait_time: int) -> dict[str, str | None]:
    """
    Wait until every dag_run in ``run_ids`` finishes, or ``max_wait_time`` elapses.

    The timeout is a budget for the whole set rather than per dag: calling
    :func:`wait_for_dag_run` in a loop restarts the clock for every dag, so one stalled run
    costs ``len(run_ids) * max_wait_time``. At the scale test_gross targets, draining every
    task is also not the point -- the scheduler's per-loop cost is fully paid once the task
    instances exist, so a fixed observation window is the measurement.
    """
    start_time = time.monotonic()
    states: dict[str, str | None] = dict.fromkeys(run_ids)

    while time.monotonic() - start_time < max_wait_time:
        with create_session() as session:
            rows = session.execute(
                select(DagRun.dag_id, DagRun.state).where(
                    tuple_(DagRun.dag_id, DagRun.run_id).in_(list(run_ids.items()))
                )
            ).all()
        states.update({dag_id: state for dag_id, state in rows})

        if all(state in (State.SUCCESS, State.FAILED) for state in states.values()):
            break

        time.sleep(2)

    return states


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


_FINISHED_TI_STATES = frozenset(state.value for state in State.finished)


class ProgressReporter:
    """
    Log a periodic state summary for ``dag_ids`` while a test waits.

    pytest holds stdout until the test ends, so a long run is indistinguishable from a
    hung one. Each line is written straight to the controlling terminal, which bypasses
    pytest's fd-level capture and needs no flags on the command line; it is also logged so
    it survives in the captured output.

    The finished-task rate is the part that answers "is it stuck": task instance counts
    can sit still legitimately while a stage barrier drains, but a rate of zero across
    several reports means nothing is moving.
    """

    def __init__(self, dag_ids: list[str], interval: int = 15):
        self._dag_ids = dag_ids
        self._interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._start_time = 0.0
        self._last_finished = 0
        self._tty: TextIO | None = None

    def __enter__(self) -> ProgressReporter:
        # Falls back to logging alone when there is no controlling terminal, e.g. CI.
        with contextlib.suppress(OSError):
            self._tty = open("/dev/tty", "w")
        self._start_time = time.monotonic()
        self._thread.start()
        return self

    def __exit__(self, *exc_info) -> None:
        self._stop.set()
        self._thread.join(timeout=self._interval + 5)
        self._report()
        if self._tty is not None:
            self._tty.close()

    def _poll(self) -> None:
        while not self._stop.wait(self._interval):
            self._report()

    def _report(self) -> None:
        with create_session() as session:
            ti_counts = session.execute(
                select(TaskInstance.state, func.count())
                .where(TaskInstance.dag_id.in_(self._dag_ids))
                .group_by(TaskInstance.state)
            ).all()
            dr_counts = session.execute(
                select(DagRun.state, func.count())
                .where(DagRun.dag_id.in_(self._dag_ids))
                .group_by(DagRun.state)
            ).all()

        finished = sum(count for state, count in ti_counts if state in _FINISHED_TI_STATES)
        rate = (finished - self._last_finished) / self._interval
        self._last_finished = finished

        line = (
            f"[progress {time.monotonic() - self._start_time:5.0f}s] "
            f"dag_runs: {format_state_counts(dr_counts)} | "
            f"tis: {format_state_counts(ti_counts)} | "
            f"finished {finished} ({rate:.1f}/s)"
        )
        log.info(line)
        if self._tty is not None:
            self._tty.write(line + "\n")
            self._tty.flush()


def format_state_counts(counts: Sequence[Any]) -> str:
    if not counts:
        return "-"
    return " ".join(f"{state or 'none'}={count}" for state, count in sorted(counts, key=lambda c: str(c[0])))


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


def get_usable_cpu_count() -> int:
    """
    CPUs this process may actually run on.

    ``os.cpu_count()`` reports the whole host even when the container is pinned to a subset,
    so sizing from it would oversubscribe a cpuset-limited box by the full difference.
    ``sched_getaffinity`` sees the pinning, but only exists on Linux. Note that neither
    reflects a CFS quota (``--cpus``): only a cpuset (``--cpuset-cpus``, systemd
    ``AllowedCPUs``) is visible from inside the container.
    """
    if hasattr(os, "sched_getaffinity"):
        return len(os.sched_getaffinity(0))
    return os.cpu_count() or 8


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
    # Purpose-built for test_many_dagruns: many concurrent small dag_runs so the
    # scheduler's DEFAULT_DAGRUNS_TO_EXAMINE (20) attention budget becomes the
    # binding constraint. Fits mac Docker: 3 workers x 10 = 30 execution slots,
    # peak DB connections ~30 + 60 (apiserver) + ~15 = ~105 under 150 ceiling.
    "manydr": {
        "worker_concurrency": "10",
        "max_tis_per_query": "100",
        "parallelism": "200",
        # Bumped from 10 to 30 so 30 concurrent dag_runs (each with one task
        # running from their linear chain) can actually parallelize instead of
        # sharing 10 per-DAG slots.
        "max_active_tasks_per_dag": "30",
        "max_active_runs_per_dag": "30",
        "default_pool_task_slot_count": "100",
    },
    # Sized so the box is NOT CPU-oversubscribed, which is what makes scheduler-level
    # changes measurable. The "manydr" profile's 30 execution slots on an 8-CPU / 8GB
    # container drove ~20 cores of demand: per-task duration inflated 5.1s -> 22.8s and
    # scheduler handoff inflated 0.65s -> 3.08s by the same factor, so every measurement
    # was dominated by contention rather than by the code under test.
    #
    # 2 workers x 4 = 8 execution slots matches the CPU count, while max_active_runs_per_dag
    # stays at 30 so the scheduler still faces more live dag_runs than its
    # DEFAULT_DAGRUNS_TO_EXAMINE (20) per-loop budget. Backlog pressure on the scheduler is
    # preserved; CPU contention is not.
    "balanced8": {
        "worker_concurrency": "4",
        "max_tis_per_query": "8",
        "parallelism": "8",
        "max_active_tasks_per_dag": "30",
        "max_active_runs_per_dag": "30",
        "default_pool_task_slot_count": "8",
    },
    # Purpose-built for test_gross: the scheduler must be the only constraint. Every cap
    # that is not the scheduler -- pool slots, parallelism, per-dag concurrency -- sits far
    # above what the workers can execute, so a task that waits is waiting on the scheduler
    # and not on a slot. The balanced8 run showed the opposite: 95% of every task's life
    # was slot wait, which measures nothing about scheduling.
    #
    # Worker slots deliberately exceed the CPU count: a no-op task here is process spawn
    # plus Execution API round-trips, so a slot spends most of its life blocked rather than
    # computing. That headroom is what keeps the worker floor from hiding scheduler wins --
    # but it is also how the old manydr profile went wrong, so treat average task exec time
    # as the tripwire. It has held at ~1.57s across runs; if it climbs with the slot count,
    # the box is oversubscribed and every measurement inflates uniformly.
    "gross": {
        # 2 celery parents x this. Raising concurrency rather than parent count on purpose:
        # celery's result_backend is Postgres, so each extra parent costs connections
        # without adding throughput.
        "worker_concurrency": str(max(2, get_usable_cpu_count() * 2 // 3)),
        # Arms measured at N=50 so far: "16" (the Airflow default) pegged 94% of loops at
        # exactly 16 enqueues and left tasks 130s in SCHEDULED while workers idled; "0"
        # ("use core.parallelism") dropped that to 0.74s but only moved the wait into the
        # broker queue, since 24 execution slots floor this box at ~1046s regardless.
        # Back to 16: the scheduler is 97% busy there, so it is the only arm where a
        # cheaper loop can show up in wall clock rather than just in the spans.
        "max_tis_per_query": "16",
        "parallelism": "4096",
        "max_active_tasks_per_dag": "4096",
        "max_active_runs_per_dag": "1",
        "default_pool_task_slot_count": "4096",
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
    # "macbook" for the laptop, "ubuntu" for the more powerful desktop, "balanced8" for
    # scheduler measurements on an 8-CPU box, "gross" for test_gross. See RESOURCE_PROFILES.
    # The profile is applied in setup_class, before `db migrate`, so it is per-class and not
    # per-test: switch back to "balanced8" before running the other tests here.
    machine_profile = "gross"

    # Mapped instances per stage for the gross dags. Total mapped task instances is
    # 35 dags * 9 stages * this, so 70 gives 22050. This is the knob for the load curve --
    # double it to double the task instance count without changing the dag shape. It also
    # sets the stage barrier width (35 * this), which is what makes max_tis_per_query bite.
    gross_mapped_task_count = 70

    # How long test_gross observes the run before giving up on it draining. The first
    # ~60s go to the deferral, so keep this well above that. Examining all 35 dag_runs per
    # loop projects the unoptimised scheduler at ~2170s, so 1800 would truncate that arm and
    # make its wall clock incomparable with the optimised one.
    gross_observation_seconds = 3600

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

    triggerer_command_args = [
        "airflow",
        "triggerer",
    ]

    # No --daemon: it detaches via DaemonContext, so Popen would own a launcher process
    # that exits immediately while the real server is orphaned to init and survives
    # _terminate_all, leaving a stale apiserver running old code across runs.
    # api.workers defaults to 1, which makes a single gunicorn worker serve every task's
    # state transitions and heartbeats. It saturates well before the scheduler does and
    # inflates all task durations uniformly, hiding whatever the test is trying to measure.
    # Airflow's own guidance is roughly one worker per core.
    apiserver_command_args = [
        "airflow",
        "api-server",
        "--port",
        "8080",
        "--workers",
        str(max(4, get_usable_cpu_count() // 3)),
    ]

    dags: dict[str, SerializedDAG] = {}

    @classmethod
    def _ensure_postgres_max_connections(cls, required: int = 300) -> None:
        """
        Make sure the metadata DB accepts enough connections for the worker fleet.

        ``max_connections`` only takes effect at server start, so a fresh postgres
        container is back at the default no matter what a previous run configured.
        ``ALTER SYSTEM`` persists the new value in the data volume; applying it still
        needs a restart, which is attempted via the docker cli and otherwise left to
        the operator -- failing here is cheaper than a scheduler crash mid-run.
        """
        env = {**os.environ, "PGPASSWORD": "airflow"}

        def query(sql: str) -> str:
            result = subprocess.run(
                ["psql", *("-h", "postgres", "-U", "postgres", "-d", "postgres"), "-tAc", sql],
                env=env,
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip()

        try:
            current = int(query("show max_connections"))
        except FileNotFoundError:
            pytest.fail("psql not found -- this test must run inside the breeze environment")
        if current >= required:
            return
        query(f"ALTER SYSTEM SET max_connections = {required}")
        restart = subprocess.run(["docker", "restart", "breeze-postgres-1"], capture_output=True, check=False)
        if restart.returncode == 0:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                with contextlib.suppress(subprocess.CalledProcessError):
                    if int(query("show max_connections")) >= required:
                        return
                time.sleep(2)
        pytest.fail(
            f"postgres max_connections={current} is below the {required} this test needs. "
            "It was raised via ALTER SYSTEM but only applies after a server restart: "
            "run `docker restart breeze-postgres-1` and re-run the test."
        )

    @classmethod
    def setup_class(cls):
        otel_host = "breeze-otel-collector"
        otel_port = 4318

        # Wait for OTel collector to be reachable before running tests.
        # This prevents flaky test failures caused by transient DNS resolution issues
        # during scheduler handoff (see https://github.com/apache/airflow/issues/61070).
        wait_for_otel_collector(otel_host, otel_port)

        # Worker slots + api-server workers peak above the postgres default of 100
        # connections, which kills the scheduler mid-run; fail fast instead.
        cls._ensure_postgres_max_connections()

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
        # Left at Airflow's default. Raising it past 20 is pure overhead here: the loop would
        # hydrate more dag_runs while max_tis_per_query still caps it at 16 enqueues, and every
        # baseline loop already hit that cap with 20 examined.
        os.environ["AIRFLOW__SCHEDULER__MAX_DAGRUNS_PER_LOOP_TO_SCHEDULE"] = "20"
        os.environ["AIRFLOW__SCHEDULER__PARSING_PROCESSES"] = "2"

        # Default is pool_size 5 + max_overflow 10, i.e. up to 15 connections for every
        # DB-connected process, which the api-server multiplies by its worker count. Left
        # alone that reaches the server's max_connections and the scheduler dies with "sorry,
        # too many clients already". 8 is well above what a worker needs concurrently while
        # keeping the worst case bounded -- going much lower trades that crash for pool
        # starvation, where checkout blocks for pool_timeout instead.
        os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE"] = "3"
        os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW"] = "5"

        # The setting above governs Airflow's engine only. Celery's result backend builds its
        # own engine from this separate option (fallback {} = SQLAlchemy defaults, so 15 per
        # process), and it is created per forked child -- the one pool that scales directly
        # with worker_concurrency. A prefork child runs one task at a time and writes one
        # result, so 2 is ample.
        os.environ["AIRFLOW__CELERY__RESULT_BACKEND_SQLALCHEMY_ENGINE_OPTIONS"] = json.dumps(
            {"pool_size": 1, "max_overflow": 1}
        )

        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{cls.dag_folder}"

        # gross/dag_factory reads this at import time, so it has to be exported before the
        # dags are parsed below and before any subprocess inherits the environment.
        os.environ["PERF_GROSS_MAPPED_COUNT"] = str(cls.gross_mapped_task_count)

        # The region dags import gross.dag_factory. Everything that parses them at runtime
        # (dag processor, workers) uses BundleDagBag, which puts the bundle root on
        # sys.path; the plain DagBag in serialize_and_get_dags does not.
        if cls.dag_folder not in sys.path:
            sys.path.append(cls.dag_folder)

        # Safe mode only imports files containing both "dag" and "airflow". The region dags
        # delegate to gross.dag_factory and mention neither, and this folder holds nothing
        # but dags, so the heuristic has no work to do here.
        os.environ["AIRFLOW__CORE__DAG_DISCOVERY_SAFE_MODE"] = "False"

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

    def test_many_dagruns(self, monkeypatch, celery_worker_env_vars, capfd, session):
        """
        Measure how fast the scheduler drains a backlog of ready dag_runs.

        Every dag_run is created QUEUED *before* any scheduler or worker exists, so the
        scheduler starts against a full backlog and the wall clock measures its drain rate
        rather than how fast the harness could hand out work. See `create_queued_dag_runs`
        for why the previous CLI-per-run loop made this test measure the harness instead.

        linear_dag is 11 sequential tasks, so each dag_run contributes exactly one runnable
        task at a time. 30 runs against the "balanced8" profile's 8 execution slots keeps the
        scheduler facing more live dag_runs than its DEFAULT_DAGRUNS_TO_EXAMINE (20) per-loop
        budget while leaving the box short of CPU saturation, so scheduler-side costs are
        visible instead of buried under contention.
        """
        os.environ["AIRFLOW__TRACES__OTEL_DEBUG_TRACES_ON"] = "True"

        processes: dict[str, subprocess.Popen] = {}

        dag_id = "linear_dag"
        num_runs = 30

        assert len(self.dags) > 0
        assert dag_id in self.dags

        run_ids = create_queued_dag_runs(dag=self.dags[dag_id], dag_id=dag_id, count=num_runs)

        try:
            # 2 workers x worker_concurrency 4 = the profile's 8 execution slots. Each extra
            # celery parent costs a prefork supervisor, so use the fewest that supply the slots.
            processes = self.start_schedulers_and_workers(num_workers=2)

            for rid in run_ids:
                wait_for_dag_run(dag_id=dag_id, run_id=rid, max_wait_time=600)

            time.sleep(10)
        finally:
            for rid in run_ids:
                print_ti_output_for_dag_run(dag_id=dag_id, run_id=rid)
            self._terminate_all(processes)

        out, err = capfd.readouterr()
        log.info("out-start --\n%s\n-- out-end", out)
        log.info("err-start --\n%s\n-- err-end", err)

    @pytest.mark.parametrize("new_ti_rescan_feature", [False, True], ids=["original_code", "new_code"])
    def test_gross(self, monkeypatch, celery_worker_env_vars, capfd, session, new_ti_rescan_feature):
        """
        Reproduce a production workload of wide mapped fan-outs behind sequential barriers.

        35 identical region dags each defer for a minute, then expand nine sequential
        stages to ``gross_mapped_task_count`` instances apiece. Every stage depends on the
        whole previous stage, so each boundary is a barrier that forces the scheduler to
        resolve dependencies across the full fan-out -- inside the transaction that holds
        ``FOR UPDATE`` on the dag_run rows.

        The test stops after ``gross_observation_seconds`` whether or not the runs drained;
        see :func:`wait_for_dag_runs` for why finishing the tasks is not the measurement.
        Progress is written to the terminal as the run goes, so a long run is
        distinguishable from a hung one.
        """
        os.environ["AIRFLOW__TRACES__OTEL_DEBUG_TRACES_ON"] = "True"

        # The A/B switch for the arm: the scheduler subprocess inherits this. True runs the
        # split finished-TI fetch, False the original single-fetch code. Parametrization
        # runs the baseline arm first, then the fix, in one pytest invocation.
        os.environ["AIRFLOW__SCHEDULER__NEW_TI_RESCAN_FEATURE"] = str(new_ti_rescan_feature)
        log.info("ARM START new_ti_rescan_feature=%s at %s", new_ti_rescan_feature, timezone.utcnow())

        processes: dict[str, subprocess.Popen] = {}
        dag_ids = [f"process_region{i}" for i in range(1, 36)]

        run_ids: dict[str, str] = {}
        for dag_id in dag_ids:
            assert dag_id in self.dags
            run_ids[dag_id] = create_queued_dag_runs(dag=self.dags[dag_id], dag_id=dag_id, count=1)[0]

        try:
            # The triggerer is required here: get_capture_tasks_parameters defers, and
            # without one every dag_run would sit in the deferred state forever.
            #
            # 3 parents x worker_concurrency. A slot is held ~3.0s per task while the task's
            # own start->end is only ~1.7s -- the rest is celery fork plus supervisor startup
            # and teardown. Worker throughput is therefore slots/3.0, and it has to stay above
            # what the scheduler can enqueue, or a cheaper scheduler just moves the wait into
            # the broker queue instead of shortening the run.
            processes = self.start_schedulers_and_workers(num_workers=3, triggerer=True)

            # Sidecar gauge for the Grafana slot-occupancy panel: celery's own count of
            # held slots, which includes the fork/supervisor overhead that the DB's
            # running state (and therefore pool.running_slots) never sees.
            # start_new_session: _terminate_all signals whole process groups, and without
            # its own group the poller shares pytest's -- killpg would take down the test
            # runner itself between parametrized arms.
            processes["slot_poller"] = subprocess.Popen(
                [sys.executable, "dev/poll_worker_slots.py"],
                env=os.environ.copy(),
                start_new_session=True,
            )

            with ProgressReporter(dag_ids=dag_ids):
                states = wait_for_dag_runs(run_ids, max_wait_time=self.gross_observation_seconds)
            log.info("dag_run states after %ds: %s", self.gross_observation_seconds, states)
        finally:
            # No print_ti_output_for_dag_run: it would dump one log file per task instance.
            self._terminate_all(processes)

    @staticmethod
    def _signal_group(proc: subprocess.Popen, sig: int) -> None:
        # Every process here forks children (gunicorn workers, celery prefork pool), and
        # signalling only the parent orphans them. Popen uses start_new_session=True so the
        # parent leads its own group and one killpg reaches the whole tree.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(os.getpgid(proc.pid), sig)

    @classmethod
    def _terminate_process(cls, proc: subprocess.Popen, timeout: int = 30) -> None:
        # Grace period covers OTel atexit flush (force_flush default: 10s);
        # SIGKILL is the fallback if the process is still alive after timeout.
        cls._signal_group(proc, signal.SIGTERM)
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            cls._signal_group(proc, signal.SIGKILL)
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
        triggerer: bool = False,
    ) -> dict[str, subprocess.Popen]:
        processes: dict[str, subprocess.Popen] = {}
        try:
            processes["scheduler_1"] = subprocess.Popen(
                self.scheduler_command_args,
                env=os.environ.copy(),
                stdout=None,
                stderr=None,
                start_new_session=True,
            )

            if second_sched:
                processes["scheduler_2"] = subprocess.Popen(
                    self.scheduler_command_args,
                    env=os.environ.copy(),
                    stdout=None,
                    stderr=None,
                    start_new_session=True,
                )

            if triggerer:
                processes["triggerer"] = subprocess.Popen(
                    self.triggerer_command_args,
                    env=os.environ.copy(),
                    stdout=None,
                    stderr=None,
                    start_new_session=True,
                )

            for i in range(1, num_workers + 1):
                worker_args = [*self.celery_command_args, "--hostname", f"worker{i}"]
                processes[f"worker_{i}"] = subprocess.Popen(
                    worker_args,
                    env=os.environ.copy(),
                    stdout=None,
                    stderr=None,
                    start_new_session=True,
                )

            processes["apiserver"] = subprocess.Popen(
                self.apiserver_command_args,
                env=os.environ.copy(),
                stdout=None,
                stderr=None,
                start_new_session=True,
            )

            # Wait to ensure processes have started.
            time.sleep(10)

            # Sanity checks. Fail fast if error.
            for name, p in processes.items():
                if p.poll() is not None:
                    raise RuntimeError(f"{name} exited early with code {p.returncode}")
        except Exception:
            # Don't leak already-started processes if any check fails.
            self._terminate_all(processes)
            raise

        return processes
