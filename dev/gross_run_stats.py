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

"""Summarise the last gross run: throughput, concurrency and where the time went."""

from __future__ import annotations

from sqlalchemy import text

from airflow.utils.session import create_session

OVERALL = """
SELECT
  count(*)                                                          AS tis,
  extract(epoch FROM (max(end_date) - min(start_date)))             AS wall_s,
  avg(extract(epoch FROM (end_date - start_date)))                  AS avg_exec_s,
  sum(extract(epoch FROM (end_date - start_date)))                  AS task_seconds,
  avg(extract(epoch FROM (start_date - queued_dttm)))               AS avg_queue_s,
  avg(extract(epoch FROM (queued_dttm - scheduled_dttm)))           AS avg_sched_to_queue_s
FROM task_instance
WHERE dag_id LIKE 'process_region%'
  AND start_date IS NOT NULL AND end_date IS NOT NULL
"""

# How many task instances were executing at each task start: the observed concurrency.
CONCURRENCY = """
WITH t AS (
  SELECT start_date, end_date FROM task_instance
  WHERE dag_id LIKE 'process_region%' AND start_date IS NOT NULL AND end_date IS NOT NULL
)
SELECT c.running, count(*) AS samples
FROM t a
CROSS JOIN LATERAL (
  SELECT count(*) AS running FROM t b
  WHERE b.start_date <= a.start_date AND b.end_date > a.start_date
) c
GROUP BY c.running ORDER BY c.running
"""

BY_STAGE = """
SELECT
  split_part(task_id, '.', 2)                             AS stage,
  count(*)                                                AS tis,
  avg(extract(epoch FROM (end_date - start_date)))        AS avg_exec_s,
  avg(extract(epoch FROM (start_date - queued_dttm)))     AS avg_queue_s
FROM task_instance
WHERE dag_id LIKE 'process_region%' AND task_id LIKE '%.%'
  AND start_date IS NOT NULL AND end_date IS NOT NULL
GROUP BY 1 ORDER BY min(start_date)
"""

with create_session() as session:
    for title, sql in (
        ("OVERALL", OVERALL),
        ("CONCURRENCY", CONCURRENCY),
        ("BY STAGE", BY_STAGE),
    ):
        rows = session.execute(text(sql))
        print(f"\n=== {title} ===")
        print(" | ".join(rows.keys()))
        for row in rows:
            print(
                " | ".join("-" if v is None else f"{v:.2f}" if isinstance(v, float) else str(v) for v in row)
            )
