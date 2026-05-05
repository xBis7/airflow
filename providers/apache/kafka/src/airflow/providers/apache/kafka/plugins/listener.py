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
import time
from datetime import datetime, timezone
from fnmatch import fnmatch
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from airflow.providers.apache.kafka.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.providers.common.compat.sdk import AirflowPlugin, conf, hookimpl
from airflow.utils.net import get_hostname

if TYPE_CHECKING:
    from confluent_kafka import Producer
    from sqlalchemy.orm import Session

    from airflow.models import DagRun, TaskInstance
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
    from airflow.utils.state import TaskInstanceState

log = logging.getLogger(__name__)

CONFIG_SECTION = "kafka_listener"
SCHEMA_VERSION = 1


# Cached configs. ``fallback`` is required so the plugin loads on older
# Airflow versions that don't know about this provider's config section.
@lru_cache(maxsize=1)
def _dag_run_events_enabled() -> bool:
    return conf.getboolean(CONFIG_SECTION, "dag_run_events_enabled", fallback="False")


@lru_cache(maxsize=1)
def _task_instance_events_enabled() -> bool:
    return conf.getboolean(CONFIG_SECTION, "task_instance_events_enabled", fallback="False")


@lru_cache(maxsize=1)
def _get_topic() -> str:
    return conf.get(CONFIG_SECTION, "topic", fallback="airflow.events")


@lru_cache(maxsize=1)
def _get_bootstrap_servers() -> str:
    return conf.get(CONFIG_SECTION, "bootstrap_servers", fallback="").strip()


@lru_cache(maxsize=1)
def _get_source() -> str:
    source_from_conf = conf.get(CONFIG_SECTION, "source", fallback="")
    # Default to the current hostname, if un-set.
    return source_from_conf or get_hostname()


def _parse_filter_patterns_to_tuple(raw: str) -> tuple[str, ...]:
    return tuple(p.strip() for p in raw.split(",") if p.strip())


@lru_cache(maxsize=1)
def _get_dag_run_dag_id_allowlist() -> tuple[str, ...]:
    return _parse_filter_patterns_to_tuple(conf.get(CONFIG_SECTION, "dag_run_dag_id_allowlist", fallback=""))


@lru_cache(maxsize=1)
def _get_dag_run_dag_id_denylist() -> tuple[str, ...]:
    return _parse_filter_patterns_to_tuple(conf.get(CONFIG_SECTION, "dag_run_dag_id_denylist", fallback=""))


@lru_cache(maxsize=1)
def _get_task_instance_dag_id_allowlist() -> tuple[str, ...]:
    return _parse_filter_patterns_to_tuple(
        conf.get(CONFIG_SECTION, "task_instance_dag_id_allowlist", fallback="")
    )


@lru_cache(maxsize=1)
def _get_task_instance_dag_id_denylist() -> tuple[str, ...]:
    return _parse_filter_patterns_to_tuple(
        conf.get(CONFIG_SECTION, "task_instance_dag_id_denylist", fallback="")
    )


@lru_cache(maxsize=1)
def _get_task_instance_task_id_allowlist() -> tuple[str, ...]:
    return _parse_filter_patterns_to_tuple(
        conf.get(CONFIG_SECTION, "task_instance_task_id_allowlist", fallback="")
    )


@lru_cache(maxsize=1)
def _get_task_instance_task_id_denylist() -> tuple[str, ...]:
    return _parse_filter_patterns_to_tuple(
        conf.get(CONFIG_SECTION, "task_instance_task_id_denylist", fallback="")
    )


@lru_cache(maxsize=1)
def _get_topic_check_timeout() -> int:
    return conf.getint(CONFIG_SECTION, "topic_check_timeout", fallback="10")


@lru_cache(maxsize=1)
def _get_topic_check_retry_interval() -> int:
    return conf.getint(CONFIG_SECTION, "topic_check_retry_interval", fallback="60")


def _is_allowed(id_to_check: str, allowlist: tuple[str, ...], denylist: tuple[str, ...]) -> bool:
    """Deny takes precedence; empty allowlist means 'allow all'."""
    if denylist and any(fnmatch(id_to_check, id_pattern) for id_pattern in denylist):
        return False
    if allowlist and not any(fnmatch(id_to_check, id_pattern) for id_pattern in allowlist):
        return False
    return True


def _dag_run_event_allowed(dag_id: str) -> bool:
    return _is_allowed(
        dag_id,
        _get_dag_run_dag_id_allowlist(),
        _get_dag_run_dag_id_denylist(),
    )


def _task_instance_event_allowed(dag_id: str, task_id: str) -> bool:
    return _is_allowed(
        dag_id,
        _get_task_instance_dag_id_allowlist(),
        _get_task_instance_dag_id_denylist(),
    ) and _is_allowed(
        task_id,
        _get_task_instance_task_id_allowlist(),
        _get_task_instance_task_id_denylist(),
    )


# confluent_kafka.Producer is not fork-safe — its background threads and
# broker sockets do not survive ``os.fork``. Keep at most one Producer per
# process and reset the cached state in the child so the next call re-inits.
# If the initialization is successful, then the value is cached,
# otherwise init is retried on an interval.
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
    # If there is a cached producer, return it, don't re-initialize.
    if _producer is not None or time.monotonic() < _producer_retry_after:
        return _producer

    result: Producer | None = None
    try:
        # The plugin is loaded by every Airflow process. The Producer is a heavy dependency, and
        # by lazily importing here, we avoid the unneeded startup cost.
        from confluent_kafka import Producer

        # No need to check if bootstrap_servers are set, because it's done when registering the listeners.
        # If not set, then there won't be any listeners set and this part of the code will never execute.
        bootstrap_servers = _get_bootstrap_servers()
        producer = Producer({"bootstrap.servers": bootstrap_servers})
        topic = _get_topic()
        list_topics_pager = producer.list_topics(timeout=_get_topic_check_timeout())
        if topic not in list_topics_pager.topics:
            log.warning(
                "Kafka listener: topic %r not found on broker %r. "
                "Listener will not publish events. Create the topic on the broker to enable publishing.",
                topic,
                bootstrap_servers,
            )
        else:
            atexit.register(_flush_producer, producer)
            log.info(
                "Kafka listener attached: pid=%s source=%r topic=%r bootstrap_servers=%r",
                os.getpid(),
                _get_source(),
                topic,
                bootstrap_servers,
            )
            result = producer
    except Exception as exc:
        log.warning(
            "Kafka listener: failed to initialize producer (%s). Listener will not publish events.",
            exc,
        )

    _producer = result
    # Setting the value to `float("inf")` will essentially
    # convert the interval check from `time.monotonic() < _producer_retry_after` to
    # `time.monotonic() < float("inf")` which will always return True.
    # So, if the producer initialization is a success, it's never retried.
    _producer_retry_after = (
        float("inf") if result is not None else time.monotonic() + _get_topic_check_retry_interval()
    )
    return result


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


# DagRun
def _produce_dr_message(event: str, dag_run: DagRun, msg: str) -> None:
    dag_id = dag_run.dag_id
    if not _dag_run_event_allowed(dag_id):
        return
    try:
        _produce_message(
            event,
            dag_id,
            dag_run.run_id,
            _get_dr_payload(dag_run, msg),
        )
    except Exception:
        log.exception("Kafka listener: %s failed", event)


def _get_dr_payload(dag_run, msg) -> dict[str, Any]:
    return {
        "dag_id": getattr(dag_run, "dag_id", None),
        "run_id": getattr(dag_run, "run_id", None),
        "run_type": str(getattr(dag_run, "run_type", "") or ""),
        "logical_date": str(getattr(dag_run, "logical_date", "") or ""),
        "start_date": str(getattr(dag_run, "start_date", "") or ""),
        "end_date": str(getattr(dag_run, "end_date", "") or ""),
        "msg": msg or "",
    }


# TaskInstance
def _produce_ti_message(
    event: str,
    previous_state: TaskInstanceState,
    task_instance: RuntimeTaskInstance | TaskInstance,
    error=None,
) -> None:
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    if not _task_instance_event_allowed(dag_id, task_id):
        return
    try:
        _produce_message(
            event,
            dag_id,
            task_instance.run_id,
            _get_ti_payload(task_instance, previous_state, error=error),
        )
    except Exception:
        log.exception("Kafka listener: %s failed", event)


def _get_ti_payload(ti, previous_state, error=None) -> dict[str, Any]:
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


def _produce_message(event: str, dag_id: str, run_id: str, payload: dict[str, Any]) -> None:
    if not _dag_run_events_enabled() and not _task_instance_events_enabled():
        return
    producer = _get_producer()
    if producer is None:
        return

    # When changing any field in the body or the payload,
    # also update the SCHEMA_VERSION to avoid breaking someone's workflow.
    body = {
        "schema_version": SCHEMA_VERSION,
        "source": _get_source(),
        "event": event,
        "timestamp": _now_iso(),
        **payload,
    }
    # Key groups all events from a single DagRun (DagRun + TaskInstance) into the same
    # Kafka partition. Within the partition, the order of the events is preserved,
    # e.g. dag_run.running > ti.running > ti.success > dag_run.success.
    key = f"{dag_id}/{run_id}".encode()
    try:
        producer.produce(
            _get_topic(),
            key=key,
            value=json.dumps(body, default=str).encode("utf-8"),
            on_delivery=_on_delivery,
        )
        producer.poll(0)
    except Exception as ex:
        log.warning("Kafka listener: failed to enqueue %s for %s/%s: %s", event, dag_id, run_id, ex)


class DagRunListener:
    """Publishes DagRun state-change event messages to Kafka."""

    @hookimpl
    def on_dag_run_running(self, dag_run: DagRun, msg: str):
        _produce_dr_message("dag_run.running", dag_run, msg)

    @hookimpl
    def on_dag_run_success(self, dag_run: DagRun, msg: str):
        _produce_dr_message("dag_run.success", dag_run, msg)

    @hookimpl
    def on_dag_run_failed(self, dag_run: DagRun, msg: str):
        _produce_dr_message("dag_run.failed", dag_run, msg)


class TaskListener:
    """Publishes TaskInstance state-change event messages to Kafka."""

    if AIRFLOW_V_3_0_PLUS:

        @hookimpl
        def on_task_instance_running(
            self, previous_state: TaskInstanceState, task_instance: RuntimeTaskInstance
        ):
            _produce_ti_message("task_instance.running", previous_state, task_instance)

        @hookimpl
        def on_task_instance_success(
            self, previous_state: TaskInstanceState, task_instance: RuntimeTaskInstance
        ):
            _produce_ti_message("task_instance.success", previous_state, task_instance)

        @hookimpl
        def on_task_instance_failed(
            self,
            previous_state: TaskInstanceState,
            task_instance: RuntimeTaskInstance,
            error: None | str | BaseException,
        ):
            _produce_ti_message("task_instance.failed", previous_state, task_instance, error=error)

        @hookimpl
        def on_task_instance_skipped(
            self, previous_state: TaskInstanceState, task_instance: RuntimeTaskInstance
        ):
            _produce_ti_message("task_instance.skipped", previous_state, task_instance)
    else:

        @hookimpl
        def on_task_instance_running(  # type: ignore[misc]
            self, previous_state: TaskInstanceState, task_instance: TaskInstance, session: Session
        ):
            _produce_ti_message("task_instance.running", previous_state, task_instance)

        @hookimpl
        def on_task_instance_success(  # type: ignore[misc]
            self, previous_state: TaskInstanceState, task_instance: TaskInstance, session: Session
        ):
            _produce_ti_message("task_instance.success", previous_state, task_instance)

        @hookimpl
        def on_task_instance_failed(  # type: ignore[misc]
            self,
            previous_state: TaskInstanceState,
            task_instance: TaskInstance,
            error: None | str | BaseException,
            session: Session,
        ):
            _produce_ti_message("task_instance.failed", previous_state, task_instance, error=error)

        @hookimpl
        def on_task_instance_skipped(  # type: ignore[misc]
            self, previous_state: TaskInstanceState, task_instance: TaskInstance, session: Session
        ):
            _produce_ti_message("task_instance.skipped", previous_state, task_instance)


def _get_enabled_listeners() -> list[object]:
    listeners: list[object] = []
    dag_run_enabled = _dag_run_events_enabled()
    task_instance_enabled = _task_instance_events_enabled()
    if (dag_run_enabled or task_instance_enabled) and not _get_bootstrap_servers():
        log.warning(
            "Kafka listener: event flags are enabled (dag_run_events_enabled=%s, "
            "task_instance_events_enabled=%s) but 'bootstrap_servers' is unset. "
            "No listeners will be registered.",
            dag_run_enabled,
            task_instance_enabled,
        )
        return listeners
    if dag_run_enabled:
        listeners.append(DagRunListener())
    if task_instance_enabled:
        listeners.append(TaskListener())
    return listeners


class KafkaListenerPlugin(AirflowPlugin):
    """Publishes Airflow DagRun and TaskInstance event messages to a defined Kafka topic."""

    name = "kafka_listener"
    listeners = _get_enabled_listeners()
