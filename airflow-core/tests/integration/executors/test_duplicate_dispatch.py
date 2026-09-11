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
"""
Duplicate dispatch must not kill a healthy running task instance.

Incident background: with two HA schedulers, a task instance could be
dispatched twice; the duplicate loses the start race against the Execution
API (409 invalid_state), dies, and its FAILED executor event makes the
scheduler fail the task instance that the winning worker is still running
(see apache/airflow#69336 and its linked issues). The duplicate *dispatch*
has since been prevented upstream (#60330, #65594) and the Celery loser's
event suppressed (#60855 + #64052); this test forces the duplicate anyway
to exercise how the surviving code paths handle it.

The scenario is identical across executors — one scheduler, two workers,
the same task instance dispatched twice, the duplicate losing the /run race
deterministically (it is armed only once the winner is RUNNING). Only the
executor differs, so the flow lives in ``_DuplicateDispatchTestBase`` and
each executor is a thin subclass.

PR #69336 has two independent changes: the scheduler heartbeat guard
(``ti_alive``) and the supervisor stand-down on ``TaskAlreadyRunningError``
(``return 0``). Each is gated behind its own env var
(``DUPLICATE_DISPATCH_SCHEDULER_FIX`` / ``DUPLICATE_DISPATCH_SUPERVISOR_FIX``)
so the test can run the full 2x2 matrix (``no_fix`` / ``supervisor_only`` /
``scheduler_only`` / ``both``) and show which change actually protects the
running task.

The invariant checked is "the scheduler did not act on the loser's stale
terminal event for the still-running task" — i.e. no ``state mismatch`` log row.

Findings this matrix demonstrates:
- LocalExecutor: only the scheduler heartbeat guard protects the task. The
  supervisor ``return 0`` does not stop the event; it converts the loser's
  FAILED into a SUCCESS, which the scheduler still clobbers the live task with
  unless the guard is on. So the heartbeat guard is the load-bearing change,
  not the redundant one.
- CeleryExecutor: protected regardless of PR #69336 by the merged provider
  ``Ignore()`` (#64052) — except that turning the supervisor fix on WITHOUT the
  guard bypasses ``Ignore()`` (the supervisor swallows the error first) and
  reintroduces interference via a spurious SUCCESS event.

Run inside breeze (postgres backend). The Celery case additionally needs the
celery integration (redis broker), which ``breeze run`` cannot enable — use
``breeze shell``:
    breeze run --backend postgres pytest .../test_duplicate_dispatch.py -k Local -xvs
    breeze shell --backend postgres --integration celery \\
        "pytest .../test_duplicate_dispatch.py -k Celery -xvs"
"""

from __future__ import annotations

import logging
import os
import subprocess
import tempfile
import time
from typing import Any

import pytest
from sqlalchemy import select

from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import State

from integration.executors.duplicate_dispatch_utils import (
    get_state_mismatch_logs,
    print_banner,
    print_incident_summary,
    print_task_logs,
)
from tests_common.test_utils.integration_setup import (
    serialize_and_get_dags,
    start_scheduler,
    terminate_process,
    unpause_trigger_dag_and_get_run_id,
    wait_for_dag_run,
)

log = logging.getLogger("integration.executors.test_duplicate_dispatch")

DAG_ID = "duplicate_dispatch_dag"
TASK_ID = "long_running_task"


class _DuplicateDispatchTestBase:
    # Subclasses set the executor and, if needed, contribute extra env / workers.
    EXECUTOR: str

    test_dir = os.path.dirname(os.path.abspath(__file__))
    dag_folder = os.path.join(test_dir, "dags")
    trigger_file = os.path.join(tempfile.gettempdir(), "duplicate_dispatch_trigger")
    marker_file = os.path.join(tempfile.gettempdir(), "duplicate_dispatch_marker")

    @classmethod
    def extra_env(cls) -> dict[str, str]:
        """Executor-specific environment (e.g. the Celery broker URL)."""
        return {}

    def start_workers(self) -> list[subprocess.Popen]:
        """External worker processes to start (Celery). LocalExecutor needs none."""
        return []

    def expect_interference(self, supervisor_fix: bool, scheduler_fix: bool) -> bool:
        """Whether the scheduler is expected to act on the loser's stale terminal event."""
        raise NotImplementedError

    @classmethod
    def setup_class(cls):
        # Snapshot and restore os.environ around the class so executor-specific
        # vars (e.g. the Celery broker URL) never leak into the other subclass.
        cls._saved_environ = os.environ.copy()

        # The pytest plugin strips AIRFLOW__*__* env vars (including the JWT secret set
        # by Breeze). Both the scheduler and api-server subprocesses must share the same
        # secret; otherwise each generates its own random key and token verification fails.
        os.environ["AIRFLOW__API_AUTH__JWT_SECRET"] = "test-secret-key-for-testing"
        os.environ["AIRFLOW__API_AUTH__JWT_ISSUER"] = "airflow"

        os.environ["AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR"] = "False"
        os.environ["AIRFLOW__SCHEDULER__PROCESSOR_POLL_INTERVAL"] = "2"

        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{cls.dag_folder}"
        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = "/dev/null"
        os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"

        # The scheduler subprocess loads the duplicating executor from this test's
        # directory; it must be importable there.
        os.environ["AIRFLOW__CORE__EXECUTOR"] = cls.EXECUTOR
        existing_pythonpath = os.environ.get("PYTHONPATH")
        os.environ["PYTHONPATH"] = (
            f"{cls.test_dir}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else cls.test_dir
        )
        os.environ["DUPLICATE_DISPATCH_DAG_ID"] = DAG_ID
        os.environ["DUPLICATE_DISPATCH_TRIGGER_FILE"] = cls.trigger_file
        os.environ["DUPLICATE_DISPATCH_MARKER_FILE"] = cls.marker_file
        os.environ.update(cls.extra_env())
        cls._cleanup_files()

        reset_command = ["airflow", "db", "reset", "--yes"]
        subprocess.run(reset_command, check=True, env=os.environ.copy())

        migrate_command = ["airflow", "db", "migrate"]
        subprocess.run(migrate_command, check=True, env=os.environ.copy())

        cls.dags = serialize_and_get_dags(dag_folder=cls.dag_folder)

    @classmethod
    def teardown_class(cls):
        cls._cleanup_files()
        os.environ.clear()
        os.environ.update(cls._saved_environ)

    @classmethod
    def _cleanup_files(cls):
        for path in (cls.trigger_file, cls.marker_file):
            if os.path.exists(path):
                os.remove(path)

    def _get_ti(self, run_id: str) -> Any | None:
        with create_session() as session:
            return session.scalar(
                select(TaskInstance).where(
                    TaskInstance.task_id == TASK_ID,
                    TaskInstance.dag_id == DAG_ID,
                    TaskInstance.run_id == run_id,
                )
            )

    def _wait_for_ti_running(self, run_id: str, max_wait_time: int) -> None:
        deadline = time.monotonic() + max_wait_time
        ti = None
        while time.monotonic() < deadline:
            ti = self._get_ti(run_id)
            if ti is not None and ti.state == State.RUNNING:
                return
            time.sleep(1)
        current = ti.state if ti is not None else None
        raise AssertionError(
            f"Task instance never reached RUNNING within {max_wait_time}s (currently {current!r})."
        )

    def _wait_for_duplicate_dispatch(self, max_wait_time: int) -> None:
        deadline = time.monotonic() + max_wait_time
        while time.monotonic() < deadline:
            if os.path.exists(self.marker_file):
                return
            time.sleep(1)
        raise AssertionError(
            f"The workload was never dispatched twice within {max_wait_time}s; "
            "the test did not exercise the scenario."
        )

    # PR #69336's two changes are gated independently so the matrix can show which one
    # actually protects the running task. Each maps to one env var read by the scheduler /
    # worker subprocesses.
    FIX_MODES = {
        "no_fix": (False, False),
        "supervisor_only": (True, False),
        "scheduler_only": (False, True),
        "both": (True, True),
    }

    @pytest.mark.execution_timeout(420)
    @pytest.mark.parametrize("mode", list(FIX_MODES), ids=list(FIX_MODES))
    def test_duplicate_dispatch_does_not_kill_running_task(self, mode):
        supervisor_fix, scheduler_fix = self.FIX_MODES[mode]
        # The gates are read by the scheduler and worker subprocesses (started below),
        # which copy os.environ at spawn time. Set them before starting any of them.
        for var, enabled in (
            ("DUPLICATE_DISPATCH_SUPERVISOR_FIX", supervisor_fix),
            ("DUPLICATE_DISPATCH_SCHEDULER_FIX", scheduler_fix),
        ):
            if enabled:
                os.environ[var] = "true"
            else:
                os.environ.pop(var, None)
        # Each parametrization triggers a fresh dag run against the same DB; clear the
        # trigger/marker files so a previous run's marker is not mistaken for this one's.
        self._cleanup_files()
        expect_interference = self.expect_interference(supervisor_fix, scheduler_fix)

        scheduler_process = None
        apiserver_process = None
        worker_processes: list[subprocess.Popen] = []
        run_id = None
        try:
            # capture_output=True inherits this process's stdout/stderr (instead of
            # DEVNULL), so the duplicate dispatch, the loser's 409, and the
            # scheduler's decision stream to the terminal when running with `pytest -s`.
            scheduler_process, apiserver_process = start_scheduler(capture_output=True)
            worker_processes = self.start_workers()

            assert DAG_ID in self.dags

            run_id = unpause_trigger_dag_and_get_run_id(dag_id=DAG_ID)

            # Arm the duplicate only once the winner is RUNNING, so the duplicate
            # deterministically loses the /run race (like a broker redelivery).
            self._wait_for_ti_running(run_id, max_wait_time=120)
            print_banner(
                "Task is RUNNING on one worker. Arming a DUPLICATE dispatch of the same task\n"
                "instance — the second worker will lose the PATCH /run race with 409 Conflict."
            )
            with open(self.trigger_file, "w") as f:
                f.write(DAG_ID)
            self._wait_for_duplicate_dispatch(max_wait_time=60)
            print_banner(
                "DUPLICATE DISPATCHED. EXPECT a TaskAlreadyRunningError from the LOSING worker\n"
                "below — that death is the 'stale failed event' the scheduler must not trust.\n"
                "Watch whether the scheduler leaves the healthy running task alone."
            )

            state = wait_for_dag_run(dag_id=DAG_ID, run_id=run_id, max_wait_time=180)

            ti = self._get_ti(run_id)
            # A "state mismatch" log row means the scheduler acted on the loser's stale
            # terminal event for the still-running task instance -- the bug. Its presence is
            # the robust signal regardless of whether the task was clobbered to FAILED or
            # (via the supervisor's return-0) to SUCCESS.
            mismatch_logs = [extra for _dttm, extra in get_state_mismatch_logs(DAG_ID, run_id)]
            if expect_interference:
                no_interference_msg = (
                    f"Expected the scheduler to act on the loser's stale event ({mode}), but it "
                    f"did not: dag run state={state!r}, ti state={ti.state!r}, no state-mismatch row."
                )
                assert mismatch_logs, no_interference_msg
            else:
                interfered_msg = (
                    "The scheduler interfered with the healthy running task after the duplicate "
                    f"dispatch's loser died ({mode}): dag run state={state!r}, ti state={ti.state!r}. "
                    f"State-mismatch log rows: {mismatch_logs!r}"
                )
                assert mismatch_logs == [], interfered_msg
                assert state == State.SUCCESS, interfered_msg
                assert ti.state == State.SUCCESS, interfered_msg
                assert ti.try_number == 1
        finally:
            if run_id is not None:
                print_incident_summary(
                    dag_id=DAG_ID, task_id=TASK_ID, run_id=run_id, marker_file=self.marker_file
                )
                print_task_logs(DAG_ID, run_id)

            for worker_process in worker_processes:
                terminate_process(worker_process)

            terminate_process(scheduler_process)
            scheduler_status = scheduler_process.poll()
            assert scheduler_status is not None, (
                "The scheduler process status is None, which means that it hasn't terminated as expected."
            )

            terminate_process(apiserver_process)
            apiserver_status = apiserver_process.poll()
            assert apiserver_status is not None, (
                "The apiserver process status is None, which means that it hasn't terminated as expected."
            )


@pytest.mark.backend("postgres")
class TestDuplicateDispatchLocalExecutor(_DuplicateDispatchTestBase):
    EXECUTOR = "duplicate_dispatch_executor.DuplicateDispatchLocalExecutor"

    def expect_interference(self, supervisor_fix: bool, scheduler_fix: bool) -> bool:
        # Only the scheduler heartbeat guard protects the running task on LocalExecutor.
        # Without it the loser's terminal event reaches the scheduler and clobbers the live
        # task -- as FAILED without the supervisor fix, or as SUCCESS with it (the supervisor
        # return-0 merely converts the event's state, it does not stop the event). So the
        # supervisor fix alone does NOT prevent interference; the heartbeat guard does.
        return not scheduler_fix


@pytest.mark.integration("celery")
@pytest.mark.backend("postgres")
class TestDuplicateDispatchCeleryExecutor(_DuplicateDispatchTestBase):
    EXECUTOR = "duplicate_dispatch_celery_executor.DuplicateDispatchCeleryExecutor"

    @classmethod
    def extra_env(cls) -> dict[str, str]:
        # The redis service is started by the breeze celery integration; the env-var
        # form of the broker URL is stripped by the pytest plugin, so re-set it.
        return {"AIRFLOW__CELERY__BROKER_URL": "redis://redis:6379/0"}

    def expect_interference(self, supervisor_fix: bool, scheduler_fix: bool) -> bool:
        # CeleryExecutor is protected regardless of PR #69336. Without the supervisor fix,
        # the loser's TaskAlreadyRunningError propagates to the provider, which raises
        # Ignore() (#64052) so no event reaches the scheduler. With the supervisor fix, the
        # loser reports SUCCESS, which the heartbeat guard would need to absorb -- so the one
        # combination that could still interfere is supervisor-fix-on / scheduler-guard-off.
        return supervisor_fix and not scheduler_fix

    def start_workers(self) -> list[subprocess.Popen]:
        return [self._start_celery_worker("worker_a"), self._start_celery_worker("worker_b")]

    @staticmethod
    def _start_celery_worker(name: str) -> subprocess.Popen:
        return subprocess.Popen(
            [
                "airflow",
                "celery",
                "worker",
                "--concurrency",
                "1",
                "--celery-hostname",
                f"{name}@%h",
                "--skip-serve-logs",
            ],
            env=os.environ.copy(),
        )
