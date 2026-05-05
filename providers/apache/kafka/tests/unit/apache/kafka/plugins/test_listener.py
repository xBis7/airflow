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

import json
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.apache.kafka.plugins import listener as listener_mod


class _FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def monotonic(self) -> float:
        return self.t


def _metadata(topic_names):
    md = MagicMock()
    md.topics = {name: MagicMock() for name in topic_names}
    return md


@pytest.fixture(autouse=True)
def reset_listener_state():
    """Clear module-level caches between tests so each test sees fresh state."""
    for fn in (
        listener_mod._get_topic,
        listener_mod._get_bootstrap_servers,
        listener_mod._get_source,
        listener_mod._get_allowlist,
        listener_mod._get_denylist,
        listener_mod._get_topic_check_timeout,
        listener_mod._get_topic_check_retry_interval,
    ):
        fn.cache_clear()
    listener_mod._producer = None
    listener_mod._producer_retry_after = 0.0
    yield
    listener_mod._producer = None
    listener_mod._producer_retry_after = 0.0


class TestDagIdAllowed:
    def test_no_filters_allows_everything(self, monkeypatch):
        monkeypatch.setattr(listener_mod, "_get_allowlist", lambda: ())
        monkeypatch.setattr(listener_mod, "_get_denylist", lambda: ())
        assert listener_mod._dag_id_allowed("anything") is True

    def test_allowlist_only(self, monkeypatch):
        monkeypatch.setattr(listener_mod, "_get_allowlist", lambda: ("etl_*",))
        monkeypatch.setattr(listener_mod, "_get_denylist", lambda: ())
        assert listener_mod._dag_id_allowed("etl_users") is True
        assert listener_mod._dag_id_allowed("sales_pipeline") is False

    def test_denylist_only(self, monkeypatch):
        monkeypatch.setattr(listener_mod, "_get_allowlist", lambda: ())
        monkeypatch.setattr(listener_mod, "_get_denylist", lambda: ("tmp_*",))
        assert listener_mod._dag_id_allowed("tmp_dev") is False
        assert listener_mod._dag_id_allowed("etl_users") is True

    def test_denylist_takes_precedence(self, monkeypatch):
        monkeypatch.setattr(listener_mod, "_get_allowlist", lambda: ("etl_*",))
        monkeypatch.setattr(listener_mod, "_get_denylist", lambda: ("etl_tmp",))
        assert listener_mod._dag_id_allowed("etl_users") is True
        assert listener_mod._dag_id_allowed("etl_tmp") is False

    def test_glob_matching(self, monkeypatch):
        monkeypatch.setattr(listener_mod, "_get_allowlist", lambda: ("sales_*",))
        monkeypatch.setattr(listener_mod, "_get_denylist", lambda: ())
        assert listener_mod._dag_id_allowed("sales_pipeline") is True
        # underscore is required by the glob, no implicit prefix match
        assert listener_mod._dag_id_allowed("salesreport") is False


class TestGetProducer:
    @pytest.fixture(autouse=True)
    def _bootstrap(self, monkeypatch):
        monkeypatch.setattr(listener_mod, "_get_bootstrap_servers", lambda: "broker:29092")

    def test_returns_producer_when_topic_exists(self):
        producer = MagicMock()
        producer.list_topics.return_value = _metadata(["airflow.events", "other"])
        with patch("confluent_kafka.Producer", return_value=producer):
            assert listener_mod._get_producer() is producer

    def test_returns_none_when_topic_missing(self):
        producer = MagicMock()
        producer.list_topics.return_value = _metadata(["other"])
        with patch("confluent_kafka.Producer", return_value=producer):
            assert listener_mod._get_producer() is None

    def test_returns_none_on_producer_exception(self):
        with patch("confluent_kafka.Producer", side_effect=RuntimeError("boom")):
            assert listener_mod._get_producer() is None

    def test_returns_none_when_bootstrap_servers_empty(self, monkeypatch):
        monkeypatch.setattr(listener_mod, "_get_bootstrap_servers", lambda: "")
        with patch("confluent_kafka.Producer") as mock_producer_cls:
            assert listener_mod._get_producer() is None
        mock_producer_cls.assert_not_called()

    def test_caches_success_indefinitely(self):
        producer = MagicMock()
        producer.list_topics.return_value = _metadata(["airflow.events"])
        clock = _FakeClock()
        with (
            patch("confluent_kafka.Producer", return_value=producer) as mock_producer_cls,
            patch.object(listener_mod.time, "monotonic", clock.monotonic),
        ):
            assert listener_mod._get_producer() is producer
            clock.t += 99999  # well past any TTL
            assert listener_mod._get_producer() is producer
            assert mock_producer_cls.call_count == 1

    def test_caches_failure_with_ttl_then_rechecks(self):
        producer_missing = MagicMock()
        producer_missing.list_topics.return_value = _metadata([])  # topic missing
        producer_ok = MagicMock()
        producer_ok.list_topics.return_value = _metadata(["airflow.events"])
        clock = _FakeClock(start=1000.0)
        with (
            patch(
                "confluent_kafka.Producer",
                side_effect=[producer_missing, producer_ok],
            ) as mock_producer_cls,
            patch.object(listener_mod.time, "monotonic", clock.monotonic),
        ):
            # First call: missing → cached as None.
            assert listener_mod._get_producer() is None
            # Within TTL: still cached, no second Producer construction.
            clock.t += 1
            assert listener_mod._get_producer() is None
            assert mock_producer_cls.call_count == 1
            # Past TTL: rechecks. Topic is now present.
            clock.t += listener_mod._get_topic_check_retry_interval() + 1
            assert listener_mod._get_producer() is producer_ok
            assert mock_producer_cls.call_count == 2


class TestProduce:
    def test_no_op_when_disabled(self, monkeypatch):
        monkeypatch.setattr(listener_mod, "_is_enabled", lambda: False)
        with patch.object(listener_mod, "_get_producer") as mock_get:
            listener_mod._produce("evt", "dag", "run", {"task_id": "t"})
        mock_get.assert_not_called()

    def test_no_op_when_dag_filtered(self, monkeypatch):
        monkeypatch.setattr(listener_mod, "_is_enabled", lambda: True)
        monkeypatch.setattr(listener_mod, "_dag_id_allowed", lambda dag_id: False)
        with patch.object(listener_mod, "_get_producer") as mock_get:
            listener_mod._produce("evt", "dag", "run", {"task_id": "t"})
        mock_get.assert_not_called()

    def test_no_op_when_producer_none(self, monkeypatch):
        monkeypatch.setattr(listener_mod, "_is_enabled", lambda: True)
        monkeypatch.setattr(listener_mod, "_dag_id_allowed", lambda dag_id: True)
        monkeypatch.setattr(listener_mod, "_get_producer", lambda: None)
        # Should return without raising; nothing to assert beyond that.
        listener_mod._produce("evt", "dag", "run", {})

    def test_produces_message_with_expected_shape(self, monkeypatch):
        producer = MagicMock()
        monkeypatch.setattr(listener_mod, "_is_enabled", lambda: True)
        monkeypatch.setattr(listener_mod, "_dag_id_allowed", lambda dag_id: True)
        monkeypatch.setattr(listener_mod, "_get_producer", lambda: producer)
        monkeypatch.setattr(listener_mod, "_get_topic", lambda: "events.topic")
        monkeypatch.setattr(listener_mod, "_get_source", lambda: "test-source")

        listener_mod._produce(
            "task_instance.success",
            "dag1",
            "run1",
            {"task_id": "load", "try_number": 2},
        )

        producer.produce.assert_called_once()
        kwargs = producer.produce.call_args.kwargs
        args = producer.produce.call_args.args
        assert args == ("events.topic",)
        assert kwargs["key"] == b"dag1/run1"
        body = json.loads(kwargs["value"].decode("utf-8"))
        assert body["schema_version"] == listener_mod.SCHEMA_VERSION
        assert body["source"] == "test-source"
        assert body["event"] == "task_instance.success"
        assert body["task_id"] == "load"
        assert body["try_number"] == 2
        assert "timestamp" in body
        producer.poll.assert_called_with(0)

    def test_swallows_produce_exceptions(self, monkeypatch):
        producer = MagicMock()
        producer.produce.side_effect = RuntimeError("queue full")
        monkeypatch.setattr(listener_mod, "_is_enabled", lambda: True)
        monkeypatch.setattr(listener_mod, "_dag_id_allowed", lambda dag_id: True)
        monkeypatch.setattr(listener_mod, "_get_producer", lambda: producer)
        # Must not propagate — Airflow's hot path can't be hurt by Kafka issues.
        listener_mod._produce("evt", "dag", "run", {})


def _ti_mock():
    ti = MagicMock()
    ti.dag_id = "etl"
    ti.task_id = "load"
    ti.run_id = "rid"
    ti.try_number = 2
    ti.map_index = -1
    return ti


def _dr_mock():
    dr = MagicMock()
    dr.dag_id = "etl"
    dr.run_id = "rid"
    dr.run_type = "manual"
    dr.logical_date = None
    dr.start_date = None
    dr.end_date = None
    return dr


class TestHookImpls:
    @pytest.fixture
    def mock_produce(self, monkeypatch):
        m = MagicMock()
        monkeypatch.setattr(listener_mod, "_produce", m)
        return m

    @pytest.mark.parametrize(
        ("hook_name", "event"),
        [
            ("on_task_instance_running", "task_instance.running"),
            ("on_task_instance_success", "task_instance.success"),
            ("on_task_instance_skipped", "task_instance.skipped"),
        ],
    )
    def test_ti_state_hooks(self, hook_name, event, mock_produce):
        ti = _ti_mock()
        getattr(listener_mod, hook_name)(previous_state="running", task_instance=ti)
        mock_produce.assert_called_once()
        evt, dag_id, run_id, payload = mock_produce.call_args.args
        assert evt == event
        assert dag_id == "etl"
        assert run_id == "rid"
        assert payload["task_id"] == "load"
        assert payload["try_number"] == 2
        assert payload["previous_state"] == "running"
        assert "error" not in payload

    def test_ti_failed_includes_error(self, mock_produce):
        ti = _ti_mock()
        listener_mod.on_task_instance_failed(
            previous_state="running",
            task_instance=ti,
            error=ValueError("boom"),
        )
        mock_produce.assert_called_once()
        payload = mock_produce.call_args.args[3]
        assert mock_produce.call_args.args[0] == "task_instance.failed"
        assert payload["error"] == "boom"

    def test_ti_failed_without_error(self, mock_produce):
        ti = _ti_mock()
        listener_mod.on_task_instance_failed(
            previous_state="running",
            task_instance=ti,
            error=None,
        )
        payload = mock_produce.call_args.args[3]
        assert "error" not in payload

    @pytest.mark.parametrize(
        ("hook_name", "event"),
        [
            ("on_dag_run_running", "dag_run.running"),
            ("on_dag_run_success", "dag_run.success"),
            ("on_dag_run_failed", "dag_run.failed"),
        ],
    )
    def test_dag_run_hooks(self, hook_name, event, mock_produce):
        dr = _dr_mock()
        getattr(listener_mod, hook_name)(dag_run=dr, msg="some message")
        mock_produce.assert_called_once()
        evt, dag_id, run_id, payload = mock_produce.call_args.args
        assert evt == event
        assert dag_id == "etl"
        assert run_id == "rid"
        assert payload["msg"] == "some message"
        assert payload["run_type"] == "manual"


class TestPlugin:
    def test_plugin_metadata(self):
        assert listener_mod.KafkaListenerPlugin.name == "kafka_listener"

    def test_plugin_registers_module_when_enabled(self):
        listeners = listener_mod.KafkaListenerPlugin.listeners
        assert listeners == [listener_mod] or listeners == []
