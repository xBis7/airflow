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

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import INVALID_SPAN

from airflow_shared.observability import traces
from airflow_shared.observability.traces import start_debug_span


@pytest.fixture
def set_debug_flag(monkeypatch):
    """Set the module-level debug flag; monkeypatch restores it on teardown."""

    def _set(value: bool):
        monkeypatch.setattr(traces, "_otel_debug_traces_on", value)

    return _set


@pytest.fixture
def in_memory_exporter(monkeypatch):
    """Use an in memory exporter and patch the module tracer to use it."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(traces, "tracer", provider.get_tracer(__name__))
    return exporter


class TestStartDebugSpan:
    @pytest.mark.parametrize(
        ("debug_flag_val", "expected_span_num"),
        [
            pytest.param(True, 1, id="flag_enabled"),
            pytest.param(False, 0, id="flag_disabled"),
        ],
    )
    def test_start_debug_span_context_manager(
        self, debug_flag_val: bool, expected_span_num: int, set_debug_flag, in_memory_exporter
    ):
        set_debug_flag(debug_flag_val)
        span_name = "ctx_mgr_test_span"

        # If the flag is enabled, then the span will be recording, else it won't be.
        is_recording = debug_flag_val

        with start_debug_span("ctx_mgr_test_span") as span:
            if expected_span_num > 0:
                expected_span = trace.get_current_span()
            else:
                expected_span = INVALID_SPAN
            assert span.is_recording() is is_recording
            assert span is expected_span
            # If the span is non-recording, enriching it is a safe no-op
            span.set_attribute("a", 1)

        finished_spans = in_memory_exporter.get_finished_spans()

        assert len(finished_spans) == expected_span_num
        if expected_span_num > 0:
            assert finished_spans[0].name == span_name
            assert finished_spans[0].attributes["a"] == 1

    @pytest.mark.parametrize(
        ("debug_flag_val", "expected_span_num"),
        [
            pytest.param(True, 1, id="flag_enabled"),
            pytest.param(False, 0, id="flag_disabled"),
        ],
    )
    def test_start_debug_span_decorator(
        self, debug_flag_val: bool, expected_span_num: int, set_debug_flag, in_memory_exporter
    ):
        span_name = "decorator_test_span"

        @start_debug_span(span_name)
        def double(x):
            trace.get_current_span().set_attribute("a", 1)
            return x * 2

        set_debug_flag(debug_flag_val)

        assert double(2) == 4

        finished_spans = in_memory_exporter.get_finished_spans()

        assert len(finished_spans) == expected_span_num
        if expected_span_num > 0:
            assert finished_spans[0].name == span_name
            assert finished_spans[0].attributes["a"] == 1
