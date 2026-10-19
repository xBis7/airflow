#
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
Sample CPU and memory of the processes the perf harness started and push them to the OTel collector.

The breeze OTel stack only receives what Airflow itself emits over OTLP: there is no host or
container metrics receiver, and every Airflow process is a subprocess of the harness inside one
container. So the harness starts this script next to those processes, passing ``role=pid``
pairs, and it samples each process tree every few seconds.

CPU comes from ``cpu_times`` deltas rather than ``Process.cpu_percent``: celery and the task
supervisor fork per task, so the tree changes between samples, and a forked child's first
``cpu_percent`` call always reads 0. Summing ``user + system + children_user + children_system``
over the root and its live descendants counts reaped children through their parent's
``children_*`` fields, so the time of a task process that lived and died between two samples
is not lost. Percent is that delta over wall time: one fully busy core reads 100.

RSS is summed over the tree and therefore double counts pages shared between a forked child and
its parent. It tracks growth and compares arms; it is not an absolute footprint.

Runs alongside the perf tests (started by the harness); exits with them.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import urllib.request

import psutil

log = logging.getLogger(__name__)

OTLP_URL = "http://breeze-otel-collector:4318/v1/metrics"
INTERVAL_SECONDS = 5.0


def parse_roles(pairs: list[str]) -> dict[str, int]:
    roles: dict[str, int] = {}
    for pair in pairs:
        role, _, pid = pair.partition("=")
        if not role or not pid.isdigit():
            raise SystemExit(f"expected ROLE=PID, got {pair!r}")
        roles[role] = int(pid)
    return roles


def sample_tree(pid: int) -> tuple[float, int] | None:
    """Return ``(cpu_seconds, rss_bytes)`` summed over ``pid`` and its live descendants."""
    try:
        root = psutil.Process(pid)
        processes = [root, *root.children(recursive=True)]
    except psutil.NoSuchProcess:
        return None
    cpu_seconds = 0.0
    rss_bytes = 0
    for process in processes:
        try:
            times = process.cpu_times()
            cpu_seconds += times.user + times.system + times.children_user + times.children_system
            rss_bytes += process.memory_info().rss
        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
            continue
    return cpu_seconds, rss_bytes


def sample_roles(roles: dict[str, int]) -> dict[str, tuple[float, int]]:
    samples: dict[str, tuple[float, int]] = {}
    for role, pid in roles.items():
        if (sample := sample_tree(pid)) is not None:
            samples[role] = sample
    return samples


def build_payload(cpu_percent: dict[str, float], rss_bytes: dict[str, int], now_ns: int) -> dict:
    def data_points(values: dict[str, float] | dict[str, int], value_key: str) -> list[dict]:
        return [
            {
                "attributes": [{"key": "role", "value": {"stringValue": role}}],
                value_key: value if value_key == "asDouble" else str(value),
                "timeUnixNano": str(now_ns),
            }
            for role, value in values.items()
        ]

    return {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [{"key": "service.name", "value": {"stringValue": "gross-poller"}}]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "gross.process.poller"},
                        "metrics": [
                            {
                                "name": "gross.process.cpu_percent",
                                "gauge": {"dataPoints": data_points(cpu_percent, "asDouble")},
                            },
                            {
                                "name": "gross.process.rss_bytes",
                                "gauge": {"dataPoints": data_points(rss_bytes, "asInt")},
                            },
                        ],
                    }
                ],
            }
        ]
    }


def push(payload: dict) -> None:
    req = urllib.request.Request(
        OTLP_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=5).read()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Push CPU and RSS of the given process trees to the OTel collector."
    )
    parser.add_argument("roles", nargs="+", metavar="ROLE=PID")
    parser.add_argument(
        "--once", action="store_true", help="print one sample as JSON to stdout and exit without pushing"
    )
    args = parser.parse_args(argv)
    roles = parse_roles(args.roles)

    previous_at = time.monotonic()
    previous = sample_roles(roles)
    interval = 1.0 if args.once else INTERVAL_SECONDS
    while True:
        time.sleep(interval)
        now_at = time.monotonic()
        current = sample_roles(roles)
        elapsed = now_at - previous_at
        cpu_percent = {
            role: max(0.0, (cpu - previous[role][0]) / elapsed * 100.0)
            for role, (cpu, _) in current.items()
            if role in previous
        }
        rss_bytes = {role: rss for role, (_, rss) in current.items()}
        if args.once:
            json.dump({"cpu_percent": cpu_percent, "rss_bytes": rss_bytes}, sys.stdout, indent=2)
            print()
            return
        try:
            push(build_payload(cpu_percent, rss_bytes, time.time_ns()))
        except Exception:
            log.exception("usage push failed; retrying next interval")
        if not current:
            log.info("every sampled process is gone; exiting")
            return
        previous, previous_at = current, now_at


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
