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

import atexit
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from fnmatch import fnmatch
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowPlugin, conf, hookimpl
from airflow.utils.net import get_hostname

if TYPE_CHECKING:
    from confluent_kafka import Producer

log = logging.getLogger(__name__)

CONFIG_SECTION = "kafka_listener"
SCHEMA_VERSION = 1


def _is_enabled() -> bool:
    return conf.getboolean(CONFIG_SECTION, "enabled", fallback=True)


@lru_cache(maxsize=1)
def _get_topic() -> str:
    return conf.get(CONFIG_SECTION, "topic", fallback="airflow.events")


@lru_cache(maxsize=1)
def _get_bootstrap_servers() -> str:
    return conf.get(CONFIG_SECTION, "bootstrap_servers", fallback="").strip()


@lru_cache(maxsize=1)
def _get_source() -> str:
    val = conf.get(CONFIG_SECTION, "source", fallback="")
    return val or get_hostname()


@lru_cache(maxsize=1)
def _get_allowlist() -> tuple[str, ...]:
    raw = conf.get(CONFIG_SECTION, "dag_id_allowlist", fallback="")
    return tuple(p.strip() for p in raw.split(",") if p.strip())


@lru_cache(maxsize=1)
def _get_denylist() -> tuple[str, ...]:
    raw = conf.get(CONFIG_SECTION, "dag_id_denylist", fallback="")
    return tuple(p.strip() for p in raw.split(",") if p.strip())


@lru_cache(maxsize=1)
def _get_topic_check_timeout() -> int:
    return conf.getint(CONFIG_SECTION, "topic_check_timeout", fallback=10)


@lru_cache(maxsize=1)
def _get_topic_check_retry_interval() -> int:
    return conf.getint(CONFIG_SECTION, "topic_check_retry_interval", fallback=60)


def _dag_id_allowed(dag_id: str) -> bool:
    deny = _get_denylist()
    if deny and any(fnmatch(dag_id, p) for p in deny):
        return False
    allow = _get_allowlist()
    if allow and not any(fnmatch(dag_id, p) for p in allow):
        return False
    return True


# confluent_kafka.Producer is not fork-safe — its background threads and
# broker sockets do not survive ``os.fork``. Keep at most one Producer per
# process and reset the cached state in the child so the next call re-inits.
# Successful inits cache forever (``_producer_retry_after = inf``); failed
# inits cache for ``topic_check_retry_interval`` seconds so the listener
# self-heals once the operator creates the topic or fixes the broker.
_producer: Producer | None = None
_producer_retry_after: float = 0.0


def _reset_producer_after_fork() -> None:
    """Drop any inherited producer in the child; the child re-inits on next call."""
    global _producer, _producer_retry_after
    _producer = None
    _producer_retry_after = 0.0


os.register_at_fork(after_in_child=_reset_producer_after_fork)


def _get_producer() -> Producer | None:
    global _producer, _producer_retry_after
    if _producer is not None or time.monotonic() < _producer_retry_after:
        return _producer

    producer: Producer | None = None
    try:
        from confluent_kafka import Producer as _Producer

        bootstrap = _get_bootstrap_servers()
        if not bootstrap:
            log.warning(
                "Kafka listener: 'bootstrap_servers' is not configured. Listener will not publish events."
            )
        else:
            producer = _Producer({"bootstrap.servers": bootstrap})
            topic = _get_topic()
            md = producer.list_topics(timeout=_get_topic_check_timeout())
            if topic not in md.topics:
                log.warning(
                    "Kafka listener: topic %r not found on broker %r. "
                    "Listener will not publish events. Create the topic on the broker to enable publishing.",
                    topic,
                    bootstrap,
                )
                producer = None
            else:
                atexit.register(_flush_producer, producer)
                log.info(
                    "Kafka listener attached: pid=%s source=%r topic=%r bootstrap_servers=%r",
                    os.getpid(),
                    _get_source(),
                    topic,
                    bootstrap,
                )
    except Exception as exc:
        log.warning(
            "Kafka listener: failed to initialize producer (%s). Listener will not publish events.",
            exc,
        )
        producer = None

    _producer = producer
    _producer_retry_after = (
        float("inf") if producer is not None else time.monotonic() + _get_topic_check_retry_interval()
    )
    return producer


def _flush_producer(producer: Producer) -> None:
    try:
        producer.flush(5)
    except Exception:
        log.debug("Kafka listener: error flushing producer on exit", exc_info=True)


def _on_delivery(err, _msg) -> None:
    if err is not None:
        log.warning("Kafka listener: delivery failed: %s", err)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _produce(event: str, dag_id: str, run_id: str, payload: dict[str, Any]) -> None:
    if not _is_enabled():
        return
    if not _dag_id_allowed(dag_id):
        return
    producer = _get_producer()
    if producer is None:
        return

    body = {
        "schema_version": SCHEMA_VERSION,
        "source": _get_source(),
        "event": event,
        "timestamp": _now_iso(),
        **payload,
    }
    key = f"{dag_id}/{run_id}".encode()
    try:
        producer.produce(
            _get_topic(),
            key=key,
            value=json.dumps(body, default=str).encode("utf-8"),
            on_delivery=_on_delivery,
        )
        producer.poll(0)
    except Exception as exc:
        log.warning("Kafka listener: failed to enqueue %s for %s/%s: %s", event, dag_id, run_id, exc)


def _ti_payload(ti, previous_state, error=None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "dag_id": getattr(ti, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": getattr(ti, "run_id", None),
        "try_number": getattr(ti, "try_number", None),
        "map_index": getattr(ti, "map_index", -1),
        "previous_state": str(previous_state) if previous_state is not None else None,
    }
    if error is not None:
        payload["error"] = str(error)
    return payload


def _dr_payload(dag_run, msg) -> dict[str, Any]:
    return {
        "dag_id": getattr(dag_run, "dag_id", None),
        "run_id": getattr(dag_run, "run_id", None),
        "run_type": str(getattr(dag_run, "run_type", "") or ""),
        "logical_date": str(getattr(dag_run, "logical_date", "") or ""),
        "start_date": str(getattr(dag_run, "start_date", "") or ""),
        "end_date": str(getattr(dag_run, "end_date", "") or ""),
        # TODO: cleanup
        # "msg": msg or "",
    }


@hookimpl
def on_task_instance_running(previous_state, task_instance):
    try:
        _produce(
            "task_instance.running",
            getattr(task_instance, "dag_id", "") or "",
            getattr(task_instance, "run_id", "") or "",
            _ti_payload(task_instance, previous_state),
        )
    except Exception:
        log.exception("Kafka listener: on_task_instance_running failed")


@hookimpl
def on_task_instance_success(previous_state, task_instance):
    try:
        _produce(
            "task_instance.success",
            getattr(task_instance, "dag_id", "") or "",
            getattr(task_instance, "run_id", "") or "",
            _ti_payload(task_instance, previous_state),
        )
    except Exception:
        log.exception("Kafka listener: on_task_instance_success failed")


@hookimpl
def on_task_instance_failed(previous_state, task_instance, error=None):
    try:
        _produce(
            "task_instance.failed",
            getattr(task_instance, "dag_id", "") or "",
            getattr(task_instance, "run_id", "") or "",
            _ti_payload(task_instance, previous_state, error=error),
        )
    except Exception:
        log.exception("Kafka listener: on_task_instance_failed failed")


@hookimpl
def on_task_instance_skipped(previous_state, task_instance):
    try:
        _produce(
            "task_instance.skipped",
            getattr(task_instance, "dag_id", "") or "",
            getattr(task_instance, "run_id", "") or "",
            _ti_payload(task_instance, previous_state),
        )
    except Exception:
        log.exception("Kafka listener: on_task_instance_skipped failed")


@hookimpl
def on_dag_run_running(dag_run, msg):
    try:
        _produce(
            "dag_run.running",
            getattr(dag_run, "dag_id", "") or "",
            getattr(dag_run, "run_id", "") or "",
            _dr_payload(dag_run, msg),
        )
    except Exception:
        log.exception("Kafka listener: on_dag_run_running failed")


@hookimpl
def on_dag_run_success(dag_run, msg):
    try:
        _produce(
            "dag_run.success",
            getattr(dag_run, "dag_id", "") or "",
            getattr(dag_run, "run_id", "") or "",
            _dr_payload(dag_run, msg),
        )
    except Exception:
        log.exception("Kafka listener: on_dag_run_success failed")


@hookimpl
def on_dag_run_failed(dag_run, msg):
    try:
        _produce(
            "dag_run.failed",
            getattr(dag_run, "dag_id", "") or "",
            getattr(dag_run, "run_id", "") or "",
            _dr_payload(dag_run, msg),
        )
    except Exception:
        log.exception("Kafka listener: on_dag_run_failed failed")


class KafkaListenerPlugin(AirflowPlugin):
    """Publishes Airflow DagRun and TaskInstance events to a Kafka topic."""

    name = "kafka_listener"
    listeners = [sys.modules[__name__]] if (_is_enabled() and _get_bootstrap_servers()) else []
