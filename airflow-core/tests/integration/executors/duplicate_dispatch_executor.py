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
LocalExecutor variant that dispatches the target task's workload a second time.

Used by the duplicate-dispatch integration test to reproduce, with one
scheduler and two real workers, what a Celery broker redelivery or an HA
double-queueing produces in production: the same task instance workload
delivered to two workers.

The duplicate is armed on demand: when the test writes the target dag_id
into the file pointed to by ``DUPLICATE_DISPATCH_TRIGGER_FILE`` (it does so
once the winner is already RUNNING, so the duplicate deterministically loses
the ``PATCH /run`` race, like a redelivered message), the executor re-queues
the workload on its next ``sync()``. A second worker picks it up, gets a
genuine 409 from the Execution API, and dies; the executor reports that
death as a FAILED event for the task instance the winner is still running.
A marker file (``DUPLICATE_DISPATCH_MARKER_FILE``) records what was sent so
the test can assert the duplicate dispatch actually happened.
"""

from __future__ import annotations

import os
from pathlib import Path

from airflow.executors.local_executor import LocalExecutor

TARGET_DAG_ENV = "DUPLICATE_DISPATCH_DAG_ID"
TRIGGER_FILE_ENV = "DUPLICATE_DISPATCH_TRIGGER_FILE"
MARKER_FILE_ENV = "DUPLICATE_DISPATCH_MARKER_FILE"


class DuplicateDispatchLocalExecutor(LocalExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._duplicable_workloads = {}
        self._duplicated = False

    def _process_workloads(self, workload_list):
        target_dag_id = os.environ.get(TARGET_DAG_ENV)
        if target_dag_id and not self._duplicated:
            for workload in workload_list:
                key = getattr(workload, "key", None)
                if getattr(key, "dag_id", None) == target_dag_id:
                    self._duplicable_workloads[key] = workload
        super()._process_workloads(workload_list)

    def sync(self) -> None:
        super().sync()
        self._maybe_dispatch_duplicate()

    def _maybe_dispatch_duplicate(self) -> None:
        if self._duplicated:
            return
        trigger_file = os.environ.get(TRIGGER_FILE_ENV)
        if not trigger_file or not os.path.exists(trigger_file):
            return
        target_dag_id = Path(trigger_file).read_text().strip()
        for key, workload in self._duplicable_workloads.items():
            if key.dag_id != target_dag_id:
                continue
            self._duplicated = True
            self.log.warning(
                "Dispatching DUPLICATE workload for %s — simulating a broker redelivery / "
                "HA double-queue; a second worker will now lose the PATCH /run race",
                key,
            )
            # Mirror what _process_workloads does for a genuine message: put the
            # workload on the activity queue and account for the unread message.
            self.activity_queue.put(workload)
            with self._unread_messages:
                self._unread_messages.value += 1
            self._check_workers()
            marker = os.environ.get(MARKER_FILE_ENV)
            if marker:
                Path(marker).write_text(repr(key))
            os.remove(trigger_file)
            return
