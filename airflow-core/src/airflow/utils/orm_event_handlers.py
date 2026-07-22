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

import logging
import os
import time
import traceback
from uuid import uuid4

from sqlalchemy import event, exc
from sqlalchemy.orm import Mapper

from airflow.configuration import conf

log = logging.getLogger(__name__)


def setup_event_handlers(engine):
    """Setups event handlers."""
    from airflow.models import import_all_models

    event.listen(Mapper, "before_configured", import_all_models, once=True)

    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info["pid"] = os.getpid()

    if engine.dialect.name == "sqlite":

        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.close()
            dbapi_connection.create_function("uuid4", 0, lambda: str(uuid4()))

    # this ensures coherence in mysql when storing datetimes (not required for postgres)
    if engine.dialect.name == "mysql":

        @event.listens_for(engine, "connect")
        def set_mysql_timezone(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("SET time_zone = '+00:00'")
            cursor.close()

    @event.listens_for(engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info["pid"] != pid:
            connection_record.connection = connection_proxy.connection = None
            raise exc.DisconnectionError(
                f"Connection record belongs to pid {connection_record.info['pid']}, "
                f"attempting to check out in pid {pid}"
            )

    if conf.getboolean("debug", "sqlalchemy_stats", fallback=False):

        @event.listens_for(engine, "before_cursor_execute")
        def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            conn.info.setdefault("query_start_time", []).append(time.perf_counter())

        @event.listens_for(engine, "after_cursor_execute")
        def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            total = time.perf_counter() - conn.info["query_start_time"].pop()
            file_name = [
                f"'{f.name}':{f.filename}:{f.lineno}"
                for f in traceback.extract_stack()
                if "sqlalchemy" not in f.filename
            ][-1]
            stack = [f for f in traceback.extract_stack() if "sqlalchemy" not in f.filename]
            stack_info = ">".join([f"{f.filename.rpartition('/')[-1]}:{f.name}" for f in stack][-3:])
            conn.info.setdefault("query_start_time", []).append(time.monotonic())
            log.info(
                "@SQLALCHEMY %s |$ %s |$ %s |$  %s ",
                total,
                file_name,
                stack_info,
                statement.replace("\n", " "),
            )

    # Attach DB query count + total duration as attributes on the currently-active
    # OTel debug span, if any. No-op when the flag is off: start_debug_span yields a
    # NonRecordingSpan whose is_recording() returns False, so we exit before writing
    # attributes and the overhead is a perf_counter call plus a dict pop.
    from opentelemetry import trace as _otel_trace

    @event.listens_for(engine, "before_cursor_execute")
    def _otel_query_start(conn, cursor, statement, parameters, context, executemany):
        conn.info.setdefault("_otel_query_start", []).append(time.perf_counter())

    @event.listens_for(engine, "after_cursor_execute")
    def _otel_query_end(conn, cursor, statement, parameters, context, executemany):
        stack = conn.info.get("_otel_query_start")
        if not stack:
            return
        elapsed_ms = (time.perf_counter() - stack.pop()) * 1000
        span = _otel_trace.get_current_span()
        if not span.is_recording():
            return
        attrs = getattr(span, "attributes", None) or {}
        span.set_attribute("db.query_count", attrs.get("db.query_count", 0) + 1)
        span.set_attribute("db.query_ms", attrs.get("db.query_ms", 0.0) + elapsed_ms)
