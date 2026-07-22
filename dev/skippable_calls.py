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
Estimate what fraction of `dagrun.task_instance_scheduling_decisions` calls could
be safely skipped by a fast-path "skip when nothing changed since last visit"
short-circuit in `DagRun.update_state`.

Proxy for the real predicate (`MAX(TI.updated_at) <= last_scheduling_decision`):
we treat a call as skippable when
  (a) the previous call for the same (dag_id, run_id) returned
      num_schedulable_tis == 0, AND
  (b) num_finished_tis and num_unfinished_tis are identical between the two
      calls (proxy for "no TI state transition crossed the finished/unfinished
      boundary").

Caveats:
- Weaker than the real `updated_at` predicate: transitions WITHIN unfinished
  states (QUEUED->RUNNING, RUNNING->DEFERRED) don't change our counts but WOULD
  bump `updated_at`. So the real fix would skip strictly FEWER calls than this
  estimate. Treat the number as an optimistic upper bound.
- The first call for each dag_run in the run cannot be skipped (no predecessor).
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict

PATH = sys.argv[1] if len(sys.argv) > 1 else "files/jaeger_loops.json"

with open(PATH) as f:
    payload = json.load(f)


def tags(span):
    return {t["key"]: t["value"] for t in span.get("tags", [])}


def parent_id(span):
    for ref in span.get("references", []):
        if ref["refType"] == "CHILD_OF":
            return ref["spanID"]
    return None


# Collect all task_instance_scheduling_decisions calls with their (dag_id, run_id).
# The parent chain: task_instance_scheduling_decisions -> update_state ->
# _schedule_dag_run (has dag_id, run_id) -> _schedule_all_dag_runs -> ...
calls = []  # list of dicts

for trace in payload["data"]:
    by_id = {s["spanID"]: s for s in trace["spans"]}
    for s in trace["spans"]:
        if s["operationName"] != "dagrun.task_instance_scheduling_decisions":
            continue
        # Walk up parents until we find one with dag_id/run_id.
        cur = s
        dag_id = run_id = None
        while cur is not None:
            t = tags(cur)
            if "airflow.dag_run.dag_id" in t:
                dag_id = t["airflow.dag_run.dag_id"]
                run_id = t.get("airflow.dag_run.run_id")
                break
            pid = parent_id(cur)
            cur = by_id.get(pid) if pid else None
        if dag_id is None or run_id is None:
            continue
        t = tags(s)
        calls.append(
            {
                "dag_id": dag_id,
                "run_id": run_id,
                "start": s["startTime"],
                "duration_us": s["duration"],
                "num_tis": t.get("airflow.dag_run.num_tis"),
                "num_sched": t.get("airflow.dag_run.num_schedulable_tis"),
                "num_unfin": t.get("airflow.dag_run.num_unfinished_tis"),
                "num_fin": t.get("airflow.dag_run.num_finished_tis"),
            }
        )

print(f"total task_instance_scheduling_decisions calls with dag_run identity: {len(calls)}")
if not calls:
    sys.exit(0)

# Group by (dag_id, run_id), sort by start.
groups = defaultdict(list)
for c in calls:
    groups[(c["dag_id"], c["run_id"])].append(c)
for k in groups:
    groups[k].sort(key=lambda c: c["start"])

# Classify each call after the first for each dag_run.
skippable_count = 0
non_skippable_count = 0
first_count = 0  # cannot skip (no predecessor)
skippable_us = 0
non_skippable_us = 0
first_us = 0

# Also track skippability broken down by "why not skippable" for the calls
# that had prev num_sched=0 but did have a count change.
count_changed_despite_empty_prev = 0

for key, seq in groups.items():
    first_count += 1
    first_us += seq[0]["duration_us"]
    for i in range(1, len(seq)):
        prev, cur = seq[i - 1], seq[i]
        cond_a = prev["num_sched"] == 0
        cond_b = prev["num_fin"] == cur["num_fin"] and prev["num_unfin"] == cur["num_unfin"]
        if cond_a and cond_b:
            skippable_count += 1
            skippable_us += cur["duration_us"]
        else:
            non_skippable_count += 1
            non_skippable_us += cur["duration_us"]
            if cond_a and not cond_b:
                count_changed_despite_empty_prev += 1

total_calls = skippable_count + non_skippable_count + first_count
total_us = skippable_us + non_skippable_us + first_us

print()
print(f"unique dag_runs visited: {len(groups)}")
print(f"calls per dag_run: mean={total_calls / len(groups):.1f}  max={max(len(v) for v in groups.values())}")
print()
print("=== Skippability breakdown ===")
print(
    f"  first call per dag_run (never skippable):  {first_count:>5}  ({first_count / total_calls:>5.1%})  {first_us / 1000:>8.1f} ms"
)
print(
    f"  non-skippable subsequent calls:            {non_skippable_count:>5}  ({non_skippable_count / total_calls:>5.1%})  {non_skippable_us / 1000:>8.1f} ms"
)
print(f"    of which had prev num_sched=0 but counts changed: {count_changed_despite_empty_prev}")
print(
    f"  SKIPPABLE (upper bound):                   {skippable_count:>5}  ({skippable_count / total_calls:>5.1%})  {skippable_us / 1000:>8.1f} ms"
)
print()
print(f"total time spent in these calls: {total_us / 1000:.1f} ms")
print(f"total time saved if all skippable calls returned early: {skippable_us / 1000:.1f} ms")

# Compare against total non-idle loop wall time to get "% of scheduler loop time reclaimed"
loop_wall_us = 0
for trace in payload["data"]:
    for s in trace["spans"]:
        if s["operationName"] == "scheduler.scheduler_loop":
            t = tags(s)
            if t.get("airflow.scheduler.loop_iteration.idle") is False:
                loop_wall_us += s["duration"]
if loop_wall_us:
    print(f"\ntotal non-idle scheduler_loop wall time: {loop_wall_us / 1000:.1f} ms")
    print(f"skippable time as fraction of scheduler wall time: {skippable_us / loop_wall_us:.1%}")

# Distribution of skippable call durations (to see if we save the tail or the mean)
if skippable_count:
    durs = sorted(
        cur["duration_us"] / 1000
        for _, seq in groups.items()
        for i in range(1, len(seq))
        for prev, cur in [(seq[i - 1], seq[i])]
        if prev["num_sched"] == 0
        and prev["num_fin"] == cur["num_fin"]
        and prev["num_unfin"] == cur["num_unfin"]
    )
    n = len(durs)
    print(
        f"\nskippable call duration (ms): mean={sum(durs) / n:.2f}  p50={durs[n // 2]:.2f}  "
        f"p95={durs[int(n * 0.95)]:.2f}  max={durs[-1]:.2f}"
    )
    non_durs = sorted(
        cur["duration_us"] / 1000
        for _, seq in groups.items()
        for i in range(1, len(seq))
        for prev, cur in [(seq[i - 1], seq[i])]
        if not (
            prev["num_sched"] == 0
            and prev["num_fin"] == cur["num_fin"]
            and prev["num_unfin"] == cur["num_unfin"]
        )
    )
    if non_durs:
        m = len(non_durs)
        print(
            f"non-skippable call duration (ms): mean={sum(non_durs) / m:.2f}  p50={non_durs[m // 2]:.2f}  "
            f"p95={non_durs[int(m * 0.95)]:.2f}  max={non_durs[-1]:.2f}"
        )
