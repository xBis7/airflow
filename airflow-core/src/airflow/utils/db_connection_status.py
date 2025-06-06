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
import socket
import time
from contextlib import closing
from typing import Final

from sqlalchemy.engine.url import make_url

from airflow.configuration import conf

logger = logging.getLogger(__name__)

# code: '-3' | msg: "Temporary failure in name resolution"
_EAI_AGAIN: Final[int] = socket.EAI_AGAIN
# code: '-2' | msg: "Name or service not known"
_EAI_NONAME: Final[int] = socket.EAI_NONAME
# code: '-5' | some non-recoverable failure
_EAI_FAIL: Final[int] = socket.EAI_FAIL


class DbConnectionStatus:
    """Return value enum for `validate_db_target_if_needed()`."""

    # The hostname resolves & the port accepts TCP.
    OK = "ok"
    # There has been some DNS blip and the connection will probably recover.
    TRANSIENT_DNS = "dns_transient_failure"
    # Some DNS failure that won't be recovered, e.g. config error, cmd typo or dead DB.
    PERMANENT_DNS = "dns_permanent_failure"
    # TCP error that occurs from a dead DB.
    CONNECTION_REFUSED = "tcp_connection_refused"


db_health_status: tuple[str, float] = (DbConnectionStatus.OK, 0.0)

# For now, this is just for testing.
# TODO: add stats.
db_retry_count: int = 0

# For testing.
force_refresh: bool = False

BASE_WAIT: float = 0.5  # seconds – first back-off step
WAIT_CAP: float = 15.0  # seconds – max back-off delay


def _sleep_backoff(attempt: int) -> None:
    delay = min(BASE_WAIT * (2**attempt), WAIT_CAP)
    time.sleep(delay)


def _check_dns_resolution_with_retries(
    host: str, retries: int, delay: float
) -> tuple[str | None, str, BaseException | None]:
    """
    Repeatedly call getaddrinfo(host) up to the number of `retries`.

    Returns (ip, status) where ip is None on failure and status is
    DbConnectionStatus.*
    """
    global db_retry_count
    # Initialize to 0 in case it has another value from previous attempts.
    db_retry_count = 0

    for i in range(1, retries + 1):
        try:
            info = socket.getaddrinfo(host, None)[0]  # take first family
            return info[4][0], DbConnectionStatus.OK, None
        except socket.gaierror as err:
            if err.errno == _EAI_AGAIN:
                db_retry_count += 1
                logger.warning("DNS transient failure for %s (attempt %d/%d)", host, i, retries)
                if db_retry_count >= retries:
                    return None, DbConnectionStatus.TRANSIENT_DNS, err
                _sleep_backoff(i)  # wait then retry validation
                continue
            # permanent
            return None, DbConnectionStatus.PERMANENT_DNS, err

    return None, DbConnectionStatus.OK, None


def _check_tcp_connectivity(ip: str, port: int, timeout_s: float) -> tuple[str, BaseException | None]:
    """Try to open a plain TCP socket to ip:port."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(timeout_s)
        try:
            sock.connect((ip, port))
            return DbConnectionStatus.OK, None
        except ConnectionRefusedError as exc1:
            return DbConnectionStatus.CONNECTION_REFUSED, exc1
        except socket.timeout as exc2:
            return DbConnectionStatus.CONNECTION_REFUSED, exc2


def check_db_connectivity_if_needed(
    dns_retries: int = 1,
    dns_backoff_s: float = 0.5,
    tcp_timeout_s: float = 2.0,
):
    global db_health_status

    # TODO: configurable? per process? different value for the scheduler, worker, webserver, etc.?
    stale_after = 10

    cached_status, ts = db_health_status
    age = time.time() - ts

    # The cached value is fresh.
    if not force_refresh and age < stale_after:
        return cached_status

    url = make_url(conf.get("database", "sql_alchemy_conn"))
    host, port = url.host, url.port or (5432 if url.drivername.startswith("postgres") else 3306)

    ip, dns_status, dns_exc = _check_dns_resolution_with_retries(host, dns_retries, dns_backoff_s)

    if dns_status != DbConnectionStatus.OK and dns_exc:
        logger.error("Database hostname %s failed DNS resolution: %s", host, dns_status)
        db_health_status = (dns_status, time.time())
        raise dns_exc

    if ip is not None:
        tcp_status, tcp_exc = _check_tcp_connectivity(ip, port, tcp_timeout_s)
        if tcp_status != DbConnectionStatus.OK and tcp_exc:
            logger.error("Database %s:%s is not accepting TCP connections: %s", ip, port, tcp_status)
            db_health_status = (tcp_status, time.time())
            raise tcp_exc

        db_health_status = (tcp_status, time.time())
