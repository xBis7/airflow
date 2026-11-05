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
Poll the metadata DB for the gross dags' backlog by run kind and push it to the OTel collector.

Starvation shows as the oldest SCHEDULED task instance of one kind getting older without bound
while the other kinds stay flat. No Airflow metric shows it: task.scheduled_duration is only
emitted when a task leaves SCHEDULED, so a starved task never produces a sample.

The kind is the run_id token test_mixed_run_types writes after ``__``, e.g.
``scheduled__hourly_003``. Every kind is pushed on every poll, with zeros for the ones that have
nothing waiting, so a gauge does not keep showing its last value once that backlog clears.

Runs alongside the perf tests (started by the harness); exits with them.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.request
from typing import Any

from sqlalchemy import text

from airflow.utils.session import create_session

log = logging.getLogger(__name__)

OTLP_URL = "http://breeze-otel-collector:4318/v1/metrics"
INTERVAL_SECONDS = 5.0
# Must match MIXED_RUN_KINDS in airflow-core/tests/integration/perf/test_perf.py.
KINDS = ("hourly", "daily", "asset", "manual")
RUN_STATES = ("queued", "running", "success", "failed")

SCHEDULED_BY_KIND = """
SELECT
  substring(dr.run_id FROM '__([a-z]+)_')                AS kind,
  count(*)                                               AS tis,
  extract(epoch FROM (now() - min(ti.scheduled_dttm)))   AS oldest_s
FROM task_instance ti
JOIN dag_run dr ON dr.dag_id = ti.dag_id AND dr.run_id = ti.run_id
WHERE ti.dag_id LIKE 'process_region%' AND ti.state = 'scheduled'
GROUP BY 1
"""

RUNS_BY_KIND = """
SELECT substring(run_id FROM '__([a-z]+)_') AS kind, state, count(*)
FROM dag_run
WHERE dag_id LIKE 'process_region%'
GROUP BY 1, 2
"""


def build_gauge(name: str, points: list[tuple[float, dict[str, str]]]) -> dict[str, Any]:
    now = str(time.time_ns())
    return {
        "name": name,
        "gauge": {
            "dataPoints": [
                {
                    "asDouble": value,
                    "timeUnixNano": now,
                    "attributes": [{"key": k, "value": {"stringValue": v}} for k, v in attributes.items()],
                }
                for value, attributes in points
            ]
        },
    }


def collect_gauges() -> list[dict[str, Any]]:
    with create_session() as session:
        scheduled = {
            kind: (float(tis), float(oldest or 0))
            for kind, tis, oldest in session.execute(text(SCHEDULED_BY_KIND))
        }
        runs = {(kind, state): float(count) for kind, state, count in session.execute(text(RUNS_BY_KIND))}
    return [
        build_gauge(
            "gross.scheduled.oldest_age_seconds",
            [(scheduled.get(kind, (0.0, 0.0))[1], {"kind": kind}) for kind in KINDS],
        ),
        build_gauge(
            "gross.scheduled.tis",
            [(scheduled.get(kind, (0.0, 0.0))[0], {"kind": kind}) for kind in KINDS],
        ),
        build_gauge(
            "gross.dag_runs",
            [
                (runs.get((kind, state), 0.0), {"kind": kind, "state": state})
                for kind in KINDS
                for state in RUN_STATES
            ],
        ),
    ]


def push_gauges(gauges: list[dict[str, Any]]) -> None:
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
                "scopeMetrics": [{"scope": {"name": "gross.scheduled.poller"}, "metrics": gauges}],
            }
        ]
    }
    req = urllib.request.Request(
        OTLP_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=5).read()


def main() -> None:
    while True:
        started = time.monotonic()
        try:
            push_gauges(collect_gauges())
        except Exception:
            log.exception("backlog poll failed; retrying next interval")
        time.sleep(max(0.5, INTERVAL_SECONDS - (time.monotonic() - started)))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
