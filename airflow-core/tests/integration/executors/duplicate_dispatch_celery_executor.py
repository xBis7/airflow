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
CeleryExecutor variant that dispatches the target task's workload a second time.

Used by the Celery duplicate-dispatch integration test to reproduce, with one
scheduler and two Celery workers, the production incident: the same task
instance sent to Celery twice with two distinct Celery task ids (what the HA
double-queueing produced, ~65ms apart in the original incident).

The duplicate is sent on demand: when the test writes the target dag_id into
the file pointed to by ``DUPLICATE_DISPATCH_TRIGGER_FILE`` (it does so once
the winner is already RUNNING, so the duplicate deterministically loses the
``PATCH /run`` race, like a broker redelivery). The duplicate gets a fresh
Celery task id (``external_executor_id`` cleared before ``apply_async``) and
— mirroring the incident's last-writer-wins overwrite — REPLACES the
original's ``AsyncResult`` in ``self.workloads``, so the executor watches
the LOSER's outcome, not the winner's. A marker file
(``DUPLICATE_DISPATCH_MARKER_FILE``) records what was sent so the test can
assert the duplicate dispatch actually happened.
"""

from __future__ import annotations

import os
from pathlib import Path

from airflow.providers.celery.executors.celery_executor import CeleryExecutor

TARGET_DAG_ENV = "DUPLICATE_DISPATCH_DAG_ID"
TRIGGER_FILE_ENV = "DUPLICATE_DISPATCH_TRIGGER_FILE"
MARKER_FILE_ENV = "DUPLICATE_DISPATCH_MARKER_FILE"


class DuplicateDispatchCeleryExecutor(CeleryExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._duplicable_workloads = {}
        self._duplicated = False

    def _process_workloads(self, workload_items):
        target_dag_id = os.environ.get(TARGET_DAG_ENV)
        if target_dag_id and not self._duplicated:
            for workload in workload_items:
                ti = getattr(workload, "ti", None)
                if ti is not None and ti.dag_id == target_dag_id:
                    self._duplicable_workloads[workload.ti.key] = workload
        super()._process_workloads(workload_items)

    def sync(self) -> None:
        super().sync()
        self._maybe_send_duplicate()

    def _maybe_send_duplicate(self) -> None:
        if self._duplicated:
            return
        trigger_file = os.environ.get(TRIGGER_FILE_ENV)
        if not trigger_file or not os.path.exists(trigger_file):
            return
        target_dag_id = Path(trigger_file).read_text().strip()
        for key, workload in self._duplicable_workloads.items():
            if key.dag_id != target_dag_id:
                continue
            from airflow.providers.celery.executors.celery_executor_utils import (
                ExceptionWithTraceback,
                send_workload_to_executor,
            )

            duplicate = workload.model_copy(deep=True)
            # A fresh Celery task id, like the incident's second scheduler
            # pre-assigning its own id for the same task instance.
            duplicate.ti.external_executor_id = None
            _, _, result = send_workload_to_executor((key, duplicate, duplicate.ti.queue, self.team_name))
            if isinstance(result, ExceptionWithTraceback):
                self.log.error("Failed to send the duplicate workload: %s", result.exception)
                return
            result.backend = self.celery_app.backend
            # Last-writer-wins, mirroring the incident: the executor now
            # watches the DUPLICATE's Celery result, not the winner's.
            self.workloads[key] = result
            self.running.add(key)
            self.log.warning(
                "Dispatched DUPLICATE Celery task %s for %s — simulating the HA double-queue; "
                "the executor now tracks the duplicate's result (last writer wins)",
                result.task_id,
                key,
            )
            marker = os.environ.get(MARKER_FILE_ENV)
            if marker:
                Path(marker).write_text(f"{key} celery_task_id={result.task_id}")
            os.remove(trigger_file)
            self._duplicated = True
            return
