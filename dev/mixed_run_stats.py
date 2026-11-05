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
Summarise a test_mixed_run_types arm by run kind: runs finished, and how long runs and tasks waited.

Usage: python dev/mixed_run_stats.py [OBSERVATION_END_ISO]

Everything is cut at the observation end (default: now). The harness stops its processes after
that, and work finishing during shutdown is not part of the measurement. A starved kind shows as
fewer completed runs and as a large age for what is still waiting at the end -- waits that never
ended are not in the completed-wait percentiles, so read both.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import text

from airflow.utils.session import create_session

KIND = "substring(dr.run_id FROM '__([a-z]+)_')"

RUNS = f"""
SELECT
  {KIND}                                                              AS kind,
  count(*)                                                            AS runs,
  count(*) FILTER (WHERE dr.end_date <= :obs_end)                     AS completed,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM (dr.end_date - dr.run_after)))
    FILTER (WHERE dr.end_date <= :obs_end)                            AS p50_latency_s,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM (dr.end_date - dr.run_after)))
    FILTER (WHERE dr.end_date <= :obs_end)                            AS p95_latency_s,
  max(extract(epoch FROM (dr.end_date - dr.run_after)))
    FILTER (WHERE dr.end_date <= :obs_end)                            AS max_latency_s,
  count(*) FILTER (WHERE dr.end_date IS NULL OR dr.end_date > :obs_end) AS unfinished,
  max(extract(epoch FROM (:obs_end - dr.run_after)))
    FILTER (WHERE dr.end_date IS NULL OR dr.end_date > :obs_end)      AS oldest_unfinished_s
FROM dag_run dr
WHERE dr.dag_id LIKE 'process_region%' AND dr.run_after <= :obs_end
GROUP BY 1 ORDER BY 1
"""

TASK_WAITS = f"""
SELECT
  {KIND}                                                              AS kind,
  count(*) FILTER (WHERE ti.queued_dttm <= :obs_end)                  AS queued_tis,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM (ti.queued_dttm - ti.scheduled_dttm)))
    FILTER (WHERE ti.queued_dttm <= :obs_end)                         AS p50_wait_s,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM (ti.queued_dttm - ti.scheduled_dttm)))
    FILTER (WHERE ti.queued_dttm <= :obs_end)                         AS p95_wait_s,
  max(extract(epoch FROM (ti.queued_dttm - ti.scheduled_dttm)))
    FILTER (WHERE ti.queued_dttm <= :obs_end)                         AS max_wait_s,
  count(*) FILTER (WHERE ti.queued_dttm IS NULL OR ti.queued_dttm > :obs_end) AS waiting_at_end,
  max(extract(epoch FROM (:obs_end - ti.scheduled_dttm)))
    FILTER (WHERE ti.queued_dttm IS NULL OR ti.queued_dttm > :obs_end) AS oldest_waiting_s
FROM task_instance ti
JOIN dag_run dr ON dr.dag_id = ti.dag_id AND dr.run_id = ti.run_id
WHERE ti.dag_id LIKE 'process_region%' AND ti.scheduled_dttm <= :obs_end
GROUP BY 1 ORDER BY 1
"""

THROUGHPUT = """
SELECT
  count(*) FILTER (WHERE ti.state = 'success' AND ti.end_date <= :obs_end)  AS finished_tis,
  extract(epoch FROM (:obs_end - min(ti.scheduled_dttm)))                   AS window_s
FROM task_instance ti
WHERE ti.dag_id LIKE 'process_region%' AND ti.scheduled_dttm <= :obs_end
"""


def format_value(value: int | float | Decimal | None) -> str:
    if value is None:
        return "-"
    if isinstance(value, int):
        return str(value)
    return f"{float(value):.1f}"


def main() -> None:
    obs_end = datetime.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else datetime.now(tz=UTC)
    print(f"observation end: {obs_end.isoformat()}")
    with create_session() as session:
        for title, sql in (("RUNS BY KIND", RUNS), ("TASK WAITS BY KIND", TASK_WAITS)):
            rows = session.execute(text(sql), {"obs_end": obs_end})
            print(f"\n=== {title} ===")
            print(" | ".join(rows.keys()))
            for kind, *values in rows:
                print(" | ".join([str(kind), *(format_value(v) for v in values)]))
        finished, window_s = session.execute(text(THROUGHPUT), {"obs_end": obs_end}).one()
    print("\n=== THROUGHPUT ===")
    rate = finished / float(window_s) if window_s else 0.0
    print(f"finished_tis | window_s | tasks_per_s\n{finished} | {float(window_s or 0):.0f} | {rate:.2f}")


if __name__ == "__main__":
    main()
