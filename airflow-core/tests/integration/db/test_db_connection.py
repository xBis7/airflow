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
import logging
import os
import shutil
import socket
import subprocess
import time

import pytest
from sqlalchemy import text

from airflow import settings
from airflow.utils import db_connection_status
from airflow.utils.db_connection_status import DbConnectionStatus

log = logging.getLogger("integration.db.test_db")


def dispose_connection_pool():
    """Dispose any cached sockets so that the next query will force a new connect."""
    settings.engine.dispose()
    # Wait for SqlAlchemy.
    time.sleep(0.5)


def make_db_test_call():
    """
    Create a session and execute a query.

    It will establish a new connection if there isn't one available.
    New connections use DNS lookup.
    """
    from airflow.utils.session import create_session

    with create_session() as session:
        session.execute(text("SELECT 1"))


def assert_query_raises_exc(expected_error_msg: str, expected_status: str, expected_retry_num: int):
    db_connection_status.force_refresh = True
    with pytest.raises(socket.gaierror, match=expected_error_msg):
        make_db_test_call()

    assert len(db_connection_status.db_health_status) == 2

    assert db_connection_status.db_health_status[0] == expected_status
    assert db_connection_status.db_retry_count == expected_retry_num


@pytest.mark.backend("postgres")
class TestDbConnectionIntegration:
    def test_dns_resolution_blip(self):
        """
        Overwrite /etc/resolv.conf with a black-hole nameserver so that
        getaddrinfo() yields EAI_AGAIN → psycopg2 prints
        'Temporary failure in name resolution'.
        """
        os.environ["AIRFLOW__DATABASE__CHECK_DB_CONNECTIVITY"] = "True"

        resolv_file = "/etc/resolv.conf"
        resolv_backup = "/tmp/resolv.conf.bak"

        # Back up the original file so that it can later be restored.
        shutil.copy(resolv_file, resolv_backup)

        try:
            # Replace the IP with a bad resolver.
            with open(resolv_file, "w", encoding="utf-8") as fh:
                fh.write("nameserver 10.255.255.1\noptions timeout:1 attempts:1 ndots:0\n")

            # New connection + DNS lookup.
            dispose_connection_pool()
            assert_query_raises_exc(
                expected_error_msg="Temporary failure in name resolution",
                expected_status=DbConnectionStatus.TRANSIENT_DNS,
                expected_retry_num=3,
            )

        finally:
            # Reset the values for the next tests.
            db_connection_status.db_health_status = (DbConnectionStatus.OK, 0.0)
            db_connection_status.db_retry_count = 0

            # Restore the original file.
            with contextlib.suppress(Exception):
                shutil.copy(resolv_backup, resolv_file)

    def test_db_disconnected(self):
        os.environ["AIRFLOW__DATABASE__CHECK_DB_CONNECTIVITY"] = "True"

        getent_result = subprocess.run(["getent hosts postgres"], shell=True, capture_output=True, text=True)
        assert "postgres" in getent_result.stdout

        db_disconnect_result = subprocess.run(
            ['docker network disconnect --force breeze_default $(docker ps -aqf "name=^breeze-postgres-1$")'],
            shell=True,
            capture_output=True,
            text=True,
        )
        assert not db_disconnect_result.stdout

        getent_no_result = subprocess.run(
            ["getent hosts postgres"], shell=True, capture_output=True, text=True
        )
        # There should be no output and the string should assert False.
        assert not getent_no_result.stdout

        try:
            # New connection + DNS lookup.
            dispose_connection_pool()
            assert_query_raises_exc(
                expected_error_msg="Name or service not known",
                expected_status=DbConnectionStatus.PERMANENT_DNS,
                expected_retry_num=0,
            )
        finally:
            # Restore db connection.
            db_connect_result = subprocess.run(
                [
                    'docker network connect --alias postgres breeze_default $(docker ps -aqf "name=^breeze-postgres-1$")'
                ],
                shell=True,
                capture_output=True,
                text=True,
            )
            assert not db_connect_result.stdout

            # Reset the values for the next tests.
            db_connection_status.db_health_status = (DbConnectionStatus.OK, 0.0)
            db_connection_status.db_retry_count = 0

    def test_invalid_hostname_in_config(self):
        os.environ["AIRFLOW__DATABASE__CHECK_DB_CONNECTIVITY"] = "True"
        os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = (
            "postgresql+psycopg2://postgres:airflow@invalid/airflow"
        )

        try:
            # New connection + DNS lookup.
            dispose_connection_pool()
            assert_query_raises_exc(
                expected_error_msg="Name or service not known",
                expected_status=DbConnectionStatus.PERMANENT_DNS,
                expected_retry_num=0,
            )
        finally:
            os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = (
                "postgresql+psycopg2://postgres:airflow@postgres/airflow"
            )

            # Reset the values for the next tests.
            db_connection_status.db_health_status = (DbConnectionStatus.OK, 0.0)
            db_connection_status.db_retry_count = 0

    def test_check_not_enabled(self):
        os.environ["AIRFLOW__DATABASE__CHECK_DB_CONNECTIVITY"] = "False"

        dispose_connection_pool()
        make_db_test_call()

        # No status checks and no retries.
        assert db_connection_status.db_health_status[0] == DbConnectionStatus.OK
        assert db_connection_status.db_retry_count == 0
