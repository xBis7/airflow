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

import inspect
import logging
import os
import re
from contextlib import redirect_stdout
from io import StringIO
from unittest import mock

import pytest
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from sqlalchemy import Column, Integer, MetaData, Table, select

from airflow.exceptions import AirflowException
from airflow.models import Base as airflow_base
from airflow.settings import engine
from airflow.utils.db import (
    _REVISION_HEADS_MAP,
    LazySelectSequence,
    _get_alembic_config,
    check_migrations,
    compare_server_default,
    compare_type,
    create_default_connections,
    downgrade,
    resetdb,
    upgradedb,
)
from airflow.utils.db_manager import RunDBManager

from tests_common.test_utils.config import conf_vars
from unit.cli.commands.test_kerberos_command import PY313

pytestmark = pytest.mark.db_test


class TestDb:
    def test_database_schema_and_sqlalchemy_model_are_in_sync(self):
        import airflow.models

        airflow.models.import_all_models()
        all_meta_data = MetaData()
        # Airflow DB
        for table_name, table in airflow_base.metadata.tables.items():
            all_meta_data._add_table(table_name, table.schema, table)
        # External DB Managers
        external_db_managers = RunDBManager()
        for dbmanager in external_db_managers._managers:
            for table_name, table in dbmanager.metadata.tables.items():
                all_meta_data._add_table(table_name, table.schema, table)
        skip_fab = PY313
        if not skip_fab:
            # FAB DB Manager
            from airflow.providers.fab.auth_manager.models.db import FABDBManager

            # test FAB models
            for table_name, table in FABDBManager.metadata.tables.items():
                all_meta_data._add_table(table_name, table.schema, table)
        else:
            print("Ignoring FAB models in Python 3.13+ as FAB is not compatible with 3.13+ yet.")
        # create diff between database schema and SQLAlchemy model
        mctx = MigrationContext.configure(
            engine.connect(),
            opts={"compare_type": compare_type, "compare_server_default": compare_server_default},
        )
        diff = compare_metadata(mctx, all_meta_data)

        # known diffs to ignore
        ignores = [
            # ignore tables created by celery
            lambda t: (t[0] == "remove_table" and t[1].name == "celery_taskmeta"),
            lambda t: (t[0] == "remove_table" and t[1].name == "celery_tasksetmeta"),
            # ignore indices created by celery
            lambda t: (t[0] == "remove_index" and t[1].name == "task_id"),
            lambda t: (t[0] == "remove_index" and t[1].name == "taskset_id"),
            # from test_security unit test
            lambda t: (t[0] == "remove_table" and t[1].name == "some_model"),
            # Ignore flask-session table/index
            lambda t: (t[0] == "remove_table" and t[1].name == "session"),
            lambda t: (t[0] == "remove_index" and t[1].name == "session_id"),
            lambda t: (t[0] == "remove_index" and t[1].name == "session_session_id_uq"),
            # sqlite sequence is used for autoincrementing columns created with `sqlite_autoincrement` option
            lambda t: (t[0] == "remove_table" and t[1].name == "sqlite_sequence"),
            # fab version table
            lambda t: (t[0] == "remove_table" and t[1].name == "alembic_version_fab"),
            # Ignore _xcom_archive table
            lambda t: (t[0] == "remove_table" and t[1].name == "_xcom_archive"),
        ]

        if skip_fab:
            ignores.append(lambda t: (t[1].name.startswith("ab_")))
            ignores.append(
                lambda t: (t[0] == "remove_index" and t[1].columns[0].table.name.startswith("ab_"))
            )

        for ignore in ignores:
            diff = [d for d in diff if not ignore(d)]
        if diff:
            print("Database schema and SQLAlchemy model are not in sync: ")
            for single_diff in diff:
                print(f"Diff: {single_diff}")
            pytest.fail("Database schema and SQLAlchemy model are not in sync")

    def test_only_single_head_revision_in_migrations(self):
        config = Config()
        config.set_main_option("script_location", "airflow:migrations")
        script = ScriptDirectory.from_config(config)

        from airflow.settings import engine

        with EnvironmentContext(
            config,
            script,
            as_sql=True,
        ) as env:
            env.configure(dialect_name=engine.dialect.name)
            # This will raise if there are multiple heads
            # To resolve, use the command `alembic merge`
            script.get_current_head()

    def test_default_connections_sort(self):
        pattern = re.compile("conn_id=[\"|'](.*?)[\"|']", re.DOTALL)
        source = inspect.getsource(create_default_connections)
        src = pattern.findall(source)
        assert sorted(src) == src

    def test_check_migrations(self):
        # Should run without error. Can't easily test the behaviour, but we can check it works
        check_migrations(0)
        check_migrations(1)

    @pytest.mark.parametrize(
        "auth, expected",
        [
            (
                {
                    (
                        "core",
                        "auth_manager",
                    ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
                },
                1,
            ),
            (
                {
                    (
                        "core",
                        "auth_manager",
                    ): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
                },
                2,
            ),
        ],
    )
    @mock.patch("alembic.command")
    def test_upgradedb(self, mock_alembic_command, auth, expected):
        if PY313 and "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager" in str(auth):
            pytest.skip(
                "Skipping test for FAB Auth Manager on Python 3.13+ as FAB is not compatible with 3.13+ yet."
            )
        with conf_vars(auth):
            upgradedb()
            mock_alembic_command.upgrade.assert_called_with(mock.ANY, revision="heads")
            assert mock_alembic_command.upgrade.call_count == expected

    @pytest.mark.parametrize(
        "from_revision, to_revision",
        [("be2bfac3da23", "e959f08ac86c"), ("ccde3e26fe78", "2e42bb497a22")],
    )
    def test_offline_upgrade_wrong_order(self, from_revision, to_revision):
        with mock.patch("airflow.utils.db.settings.engine.dialect"):
            with mock.patch("alembic.command.upgrade"):
                with pytest.raises(ValueError, match="Error while checking history for revision range *:*"):
                    upgradedb(from_revision=from_revision, to_revision=to_revision, show_sql_only=True)

    @pytest.mark.parametrize(
        "to_revision, from_revision",
        [
            ("e959f08ac86c", "e959f08ac86c"),
        ],
    )
    def test_offline_upgrade_revision_nothing(self, from_revision, to_revision):
        with mock.patch("airflow.utils.db.settings.engine.dialect"):
            with mock.patch("alembic.command.upgrade"):
                with redirect_stdout(StringIO()) as temp_stdout:
                    upgradedb(to_revision=to_revision, from_revision=from_revision, show_sql_only=True)
                stdout = temp_stdout.getvalue()
                assert "nothing to do" in stdout

    @mock.patch("airflow.utils.db._offline_migration")
    @mock.patch("airflow.utils.db._get_current_revision")
    def test_offline_upgrade_no_versions(self, mock_gcr, mock_om, caplog):
        """Offline upgrade should work with no version / revision options."""
        with mock.patch("airflow.utils.db.settings.engine.dialect") as dialect:
            dialect.name = "postgresql"  # offline migration supported with postgres
            mock_gcr.return_value = "22ed7efa9da2"
            upgradedb(from_revision=None, to_revision=None, show_sql_only=True)
            actual = mock_om.call_args.args[2]
            assert re.match(r"22ed7efa9da2:[a-z0-9]+", actual) is not None

    @mock.patch("airflow.utils.db._get_current_revision")
    def test_sqlite_offline_upgrade_raises_with_revision(self, mock_gcr):
        with mock.patch("airflow.utils.db.settings.engine.dialect") as dialect:
            dialect.name = "sqlite"
            with pytest.raises(SystemExit, match="Offline migration not supported for SQLite"):
                upgradedb(from_revision=None, to_revision=None, show_sql_only=True)

    @mock.patch("airflow.utils.db._offline_migration")
    def test_downgrade_sql_no_from(self, mock_om):
        downgrade(to_revision="abc", show_sql_only=True, from_revision=None)
        actual = mock_om.call_args.kwargs["revision"]
        assert re.match(r"[a-z0-9]+:abc", actual) is not None

    @mock.patch("airflow.utils.db._offline_migration")
    def test_downgrade_sql_with_from(self, mock_om):
        downgrade(to_revision="abc", show_sql_only=True, from_revision="123")
        actual = mock_om.call_args.kwargs["revision"]
        assert actual == "123:abc"

    @mock.patch("alembic.command.downgrade")
    def test_downgrade_invalid_combo(self, mock_om):
        """Can't combine `sql=False` and `from_revision`"""
        with pytest.raises(ValueError, match="can't be combined"):
            downgrade(to_revision="abc", from_revision="123")

    @mock.patch("alembic.command.downgrade")
    def test_downgrade_with_from(self, mock_om):
        downgrade(to_revision="abc")
        actual = mock_om.call_args.kwargs["revision"]
        assert actual == "abc"

    def test_resetdb_logging_level(self):
        unset_logging_level = logging.root.level
        logging.root.setLevel(logging.DEBUG)
        set_logging_level = logging.root.level
        resetdb()
        assert logging.root.level == set_logging_level
        assert logging.root.level != unset_logging_level

    def test_alembic_configuration(self):
        with mock.patch.dict(
            os.environ, {"AIRFLOW__DATABASE__ALEMBIC_INI_FILE_PATH": "/tmp/alembic.ini"}, clear=True
        ):
            config = _get_alembic_config()
            assert config.config_file_name == "/tmp/alembic.ini"

        # default behaviour
        config = _get_alembic_config()
        import airflow

        assert config.config_file_name == os.path.join(os.path.dirname(airflow.__file__), "alembic.ini")

    def test_bool_lazy_select_sequence(self):
        class MockSession:
            def __init__(self):
                pass

            def scalar(self, stmt):
                return None

        t = Table("t", MetaData(), Column("id", Integer, primary_key=True))
        lss = LazySelectSequence.from_select(select(t.c.id), order_by=[], session=MockSession())

        assert bool(lss) is False

    @conf_vars({("core", "unit_test_mode"): "False"})
    @mock.patch("airflow.utils.db.inspect")
    def test_downgrade_raises_if_lower_than_v3_0_0_and_no_ab_user(self, mock_inspect):
        mock_inspect.return_value.has_table.return_value = False
        msg = (
            "Downgrade to revision less than 3.0.0 requires that `ab_user` table is present. "
            "Please add FabDBManager to [core] external_db_managers and run fab migrations before "
            "proceeding"
        )
        with pytest.raises(AirflowException, match=re.escape(msg)):
            downgrade(to_revision=_REVISION_HEADS_MAP["2.7.0"])
