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
Scratch toggle and measurements for the performance harness, see airflow-core/tests/integration/performance.

Not for upstream: drop this module and every use of it together with the rest of the testing commit.
"""

from __future__ import annotations

import os
from collections import Counter, defaultdict

from airflow._shared.observability.metrics import stats

# Read once: the harness sets it before it starts the scheduler, and reading the environment on
# every lookup would add the same cost to both test cases.
NEW_CODE = os.environ.get("PERF_TEST__NEW_CODE", "").lower() == "true"

# Per lookup site, accumulated over one Dag run's scheduling pass and emitted by
# DagRun.update_state, so neither test case pays a metric call per task instance.
lookups: Counter[str] = Counter()
examined: Counter[str] = Counter()
lookup_seconds: defaultdict[str, float] = defaultdict(float)
index_counts: Counter[str] = Counter()
index_seconds: defaultdict[str, float] = defaultdict(float)
dep_seconds: defaultdict[str, float] = defaultdict(float)


def record_lookup(site: str, seconds: float, examined_tis: int) -> None:
    """Record one search of the finished task instances by a dependency."""
    lookups[site] += 1
    examined[site] += examined_tis
    lookup_seconds[site] += seconds


def record_index_build(indexed_tis: int, seconds: float) -> None:
    """Record one build of the finished task instance index, which only the new code does."""
    index_counts["builds"] += 1
    index_counts["indexed"] += indexed_tis
    index_seconds["build"] += seconds


def emit_perf_metrics() -> None:
    """Emit and reset what was gathered since the last call."""
    for site, count in lookups.items():
        tags = {"site": site}
        stats.incr("perf.finished_tis.lookups", count, tags=tags)
        stats.incr("perf.finished_tis.examined", examined[site], tags=tags)
        stats.timing("perf.finished_tis.lookup_duration", lookup_seconds[site] * 1000, tags=tags)
    # Emitted in both test cases, so the original code shows zero builds rather than no series.
    stats.incr("perf.finished_tis.index_builds", index_counts["builds"])
    stats.incr("perf.finished_tis.indexed", index_counts["indexed"])
    if index_counts["builds"]:
        stats.timing("perf.finished_tis.index_duration", index_seconds["build"] * 1000)
    for dep, seconds in dep_seconds.items():
        stats.timing("perf.dep_duration", seconds * 1000, tags={"dep": dep})
    lookups.clear()
    examined.clear()
    lookup_seconds.clear()
    index_counts.clear()
    index_seconds.clear()
    dep_seconds.clear()
