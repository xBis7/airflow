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
from __future__ import annotations

import contextlib
import time
from collections import Counter, defaultdict
from typing import TYPE_CHECKING

import attr
from sqlalchemy import event
from sqlalchemy.engine import Engine

from airflow.exceptions import TaskNotFound
from airflow.stats import Stats
from airflow.utils.state import State

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow.models.dagrun import DagRun, SchedulingTI
    from airflow.models.mappedoperator import MappedOperator
    from airflow.serialization.definitions.taskgroup import SerializedTaskGroup
    from airflow.serialization.serialized_objects import SerializedBaseOperator

# Scratch measurements for the performance harness; DagRun.update_state emits them once per Dag run,
# so the memo arm does not pay one metric call per lookup.
perf_counts: Counter[str] = Counter()
perf_seconds: defaultdict[str, float] = defaultdict(float)
perf_dep_seconds: defaultdict[str, float] = defaultdict(float)


# Every statement of the process, so what the scheduler emits covers its whole loop, not only the
# dependency checks.
@event.listens_for(Engine, "before_cursor_execute")
def _start_query_timer(conn, cursor, statement, parameters, context, executemany) -> None:
    perf_counts["db_queries"] += 1
    conn.info.setdefault("perf_query_starts", []).append(time.monotonic())


@event.listens_for(Engine, "after_cursor_execute")
def _stop_query_timer(conn, cursor, statement, parameters, context, executemany) -> None:
    # Empty for a statement already running in another thread when this module was imported.
    if starts := conn.info.get("perf_query_starts"):
        perf_seconds["db_queries"] += time.monotonic() - starts.pop()


def emit_perf_metrics() -> None:
    """Emit and reset the measurements gathered since the last call."""
    Stats.incr("perf.mapped_ti_count.db_lookups", perf_counts.pop("db_lookups", 0))
    Stats.incr("perf.mapped_ti_count.memo_hits", perf_counts.pop("memo_hits", 0))
    Stats.timing("perf.mapped_ti_count.duration", perf_seconds.pop("mapped_ti_count", 0.0) * 1000)
    Stats.incr("perf.scheduler.db_queries", perf_counts.pop("db_queries", 0))
    Stats.timing("perf.scheduler.db_duration", perf_seconds.pop("db_queries", 0.0) * 1000)
    for dep, seconds in perf_dep_seconds.items():
        Stats.timing("perf.dep_duration", seconds * 1000, tags={"dep": dep})
    perf_dep_seconds.clear()


def get_counted_mapped_ti_count(
    task: MappedOperator | SerializedBaseOperator | SerializedTaskGroup,
    run_id: str,
    *,
    session: Session,
) -> int:
    """Return ``get_mapped_ti_count``, counted and timed as a database lookup."""
    from airflow.models.mappedoperator import get_mapped_ti_count

    start = time.monotonic()
    try:
        perf_counts["db_lookups"] += 1
        return get_mapped_ti_count(task, run_id, session=session)
    finally:
        perf_seconds["mapped_ti_count"] += time.monotonic() - start


@attr.define
class DepContext:
    """
    A base class for dependency contexts.

    Specifies which dependencies should be evaluated in the context for a task
    instance to satisfy the requirements of the context. Also stores state
    related to the context that can be used by dependency classes.

    For example there could be a SomeRunContext that subclasses this class which has
    dependencies for:

    - Making sure there are slots available on the infrastructure to run the task instance
    - A task-instance's task-specific dependencies are met (e.g. the previous task
      instance completed successfully)
    - ...

    :param deps: The context-specific dependencies that need to be evaluated for a
        task instance to run in this execution context.
    :param flag_upstream_failed: This is a hack to generate the upstream_failed state
        creation while checking to see whether the task instance is runnable. It was the
        shortest path to add the feature. This is bad since this class should be pure (no
        side effects).
    :param ignore_all_deps: Whether or not the context should ignore all ignorable
        dependencies. Overrides the other ignore_* parameters
    :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs (e.g. for
        Backfills)
    :param wait_for_past_depends_before_skipping: Wait for past depends before marking the ti as skipped
    :param ignore_in_retry_period: Ignore the retry period for task instances
    :param ignore_in_reschedule_period: Ignore the reschedule period for task instances
    :param ignore_unmapped_tasks: Ignore errors about mapped tasks not yet being expanded
    :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past and
        trigger rule
    :param ignore_ti_state: Ignore the task instance's previous failure/success
    :param finished_tis: A list of all the finished task instances of this run
    """

    deps: set = attr.ib(factory=set)
    flag_upstream_failed: bool = False
    ignore_all_deps: bool = False
    ignore_depends_on_past: bool = False
    wait_for_past_depends_before_skipping: bool = False
    ignore_in_retry_period: bool = False
    ignore_in_reschedule_period: bool = False
    ignore_task_deps: bool = False
    ignore_ti_state: bool = False
    ignore_unmapped_tasks: bool = False
    finished_tis: list[SchedulingTI] | None = None
    description: str | None = None

    have_changed_ti_states: bool = False
    """Have any of the TIs state's been changed as a result of evaluating dependencies"""

    mapped_ti_counts: dict[tuple[str, str, str], int] = attr.ib(factory=dict, repr=False)
    """
    Per-pass memo of mapped task instance counts, keyed by ``(dag_id, run_id, node_id)``,
    so the copies of a mapped task group share one count lookup instead of each copy running its own.

    This is deliberately an ``init=True`` field even though callers never pass it. ``attrs.evolve``
    only carries over fields that ``__init__`` accepts, and
    :meth:`~airflow.models.taskinstance.TaskInstance.are_dependencies_met` evolves the context for
    every ``UP_FOR_RESCHEDULE`` task instance. With ``init=False`` those instances would each get a
    fresh empty dict, so they would neither read the memo nor warm it for anything else.
    """

    def get_mapped_ti_count(
        self,
        task: MappedOperator | SerializedBaseOperator | SerializedTaskGroup,
        run_id: str,
        *,
        session: Session,
    ) -> int:
        """
        Return :func:`~airflow.models.mappedoperator.get_mapped_ti_count`, memoized.

        A count that cannot be computed yet raises and is not stored, so it is looked up again later.
        """
        from airflow.models.mappedoperator import get_mapped_ti_count

        start = time.monotonic()
        try:
            key = (task.dag_id, run_id, task.node_id)
            if (count := self.mapped_ti_counts.get(key)) is None:
                perf_counts["db_lookups"] += 1
                count = get_mapped_ti_count(task, run_id, session=session)
                self.mapped_ti_counts[key] = count
            else:
                perf_counts["memo_hits"] += 1
            return count
        finally:
            perf_seconds["mapped_ti_count"] += time.monotonic() - start

    def ensure_finished_tis(self, dag_run: DagRun, session: Session) -> list[SchedulingTI]:
        """
        Ensure finished_tis is populated if it's currently None, which allows running tasks without dag_run.

         :param dag_run: The DagRun for which to find finished tasks
         :return: A list of all the finished tasks of this DAG and logical_date
        """
        finished_tis: list[SchedulingTI]
        if self.finished_tis is None:
            fetched = dag_run.get_task_instances(state=State.finished, session=session)
            for ti in fetched:
                if getattr(ti, "task", None) is not None or (dag := dag_run.dag) is None:
                    continue
                with contextlib.suppress(TaskNotFound):
                    ti.task = dag.get_task(ti.task_id)
            finished_tis = list(fetched)
            self.finished_tis = finished_tis
        else:
            finished_tis = self.finished_tis
        return finished_tis
