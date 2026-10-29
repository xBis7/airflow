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

from types import SimpleNamespace
from unittest import mock

import attrs
import pytest
from sqlalchemy.orm import Session

from airflow.models.dagrun import DagRun
from airflow.ti_deps.dep_context import DepContext


def _finished_tis(*task_ids: str) -> list[SimpleNamespace]:
    return [SimpleNamespace(task_id=task_id, map_index=i) for i, task_id in enumerate(task_ids)]


@pytest.fixture
def dag_run():
    return mock.create_autospec(DagRun, instance=True)


@pytest.fixture
def session():
    return mock.create_autospec(Session, instance=True)


class TestGetFinishedTisByTaskId:
    @pytest.mark.parametrize(
        ("task_ids", "expected_positions"),
        [
            pytest.param(("a", "b", "a"), {"a": [0, 2], "b": [1]}, id="grouped"),
            pytest.param((), {}, id="nothing-finished"),
        ],
    )
    def test_groups_the_finished_tis_by_task_id(self, dag_run, session, task_ids, expected_positions):
        finished_tis = _finished_tis(*task_ids)
        dep_context = DepContext(finished_tis=finished_tis)

        by_task_id = dep_context.get_finished_tis_by_task_id(dag_run, session=session)

        assert by_task_id == {
            task_id: [finished_tis[i] for i in positions] for task_id, positions in expected_positions.items()
        }

    @mock.patch.object(DepContext, "ensure_finished_tis", autospec=True)
    def test_is_built_once_per_context(self, mock_ensure_finished_tis, dag_run, session):
        mock_ensure_finished_tis.return_value = _finished_tis("a")
        dep_context = DepContext()

        first = dep_context.get_finished_tis_by_task_id(dag_run, session=session)
        second = dep_context.get_finished_tis_by_task_id(dag_run, session=session)

        assert second is first
        mock_ensure_finished_tis.assert_called_once_with(dep_context, dag_run, session=session)

    def test_is_carried_over_by_evolve(self, dag_run, session):
        dep_context = DepContext(finished_tis=_finished_tis("a"))
        by_task_id = dep_context.get_finished_tis_by_task_id(dag_run, session=session)

        evolved = attrs.evolve(dep_context, deps=set())

        assert evolved.get_finished_tis_by_task_id(dag_run, session=session) is by_task_id

    def test_is_rebuilt_after_invalidation(self, dag_run, session):
        finished_tis = _finished_tis("a")
        dep_context = DepContext(finished_tis=finished_tis)
        stale = dep_context.get_finished_tis_by_task_id(dag_run, session=session)
        finished_tis[0] = SimpleNamespace(task_id="a", map_index=0, state="removed")

        dep_context.invalidate_upstream_task_id_counts()

        fresh = dep_context.get_finished_tis_by_task_id(dag_run, session=session)
        assert fresh is not stale
        assert fresh == {"a": [finished_tis[0]]}
