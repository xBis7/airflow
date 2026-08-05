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
Poll celery for occupied worker slots and push the count to the OTel collector.

``pool.running_slots`` only covers a task's execution phase (start_date -> end_date),
which is ~46% of the time a task actually holds a celery slot -- the rest is fork and
supervisor startup/teardown, invisible to the DB. Celery's ``inspect active`` is the
worker's own accounting of held slots, so its total is the true occupancy that
utilization graphs should show against the slot budget.

Runs alongside the perf tests (started by the harness); exits with them.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request

from celery import Celery

log = logging.getLogger(__name__)

OTLP_URL = os.environ.get("SLOT_POLLER_OTLP_URL", "http://breeze-otel-collector:4318/v1/metrics")
INTERVAL_SECONDS = 5.0


def push_gauge(value: int) -> None:
    payload = {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {"stringValue": "gross-poller"},
                        }
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "gross.worker.poller"},
                        "metrics": [
                            {
                                "name": "gross.worker.slots_occupied",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "asInt": str(value),
                                            "timeUnixNano": str(time.time_ns()),
                                        }
                                    ]
                                },
                            }
                        ],
                    }
                ],
            }
        ]
    }
    req = urllib.request.Request(
        OTLP_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=5).read()


def count_occupied_slots(app: Celery) -> int:
    active = app.control.inspect(timeout=2.0).active() or {}
    return sum(len(tasks) for tasks in active.values())


def main() -> None:
    app = Celery(broker=os.environ.get("AIRFLOW__CELERY__BROKER_URL", "redis://redis:6379/0"))
    while True:
        started = time.monotonic()
        try:
            push_gauge(count_occupied_slots(app))
        except Exception:
            log.exception("slot poll failed; retrying next interval")
        time.sleep(max(0.5, INTERVAL_SECONDS - (time.monotonic() - started)))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
