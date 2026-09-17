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
"""Stats backend that forwards metrics from a supervised subprocess to its supervisor."""

from __future__ import annotations

import datetime
import logging
import random
import threading
from typing import TYPE_CHECKING, Any

from airflow.sdk._shared.observability.metrics.protocols import Timer
from airflow.sdk.api.datamodels._generated import ForwardedMetric, MetricKind
from airflow.sdk.execution_time.comms import ForwardMetrics

if TYPE_CHECKING:
    from airflow.sdk._shared.observability.metrics.protocols import DeltaType
    from airflow.sdk.execution_time.comms import CommsDecoder

log = logging.getLogger(__name__)

FLUSH_INTERVAL_SECONDS = 30
# Bounds on a single batch: exceeding either sends the batch early instead of growing it further.
MAX_POINTS = 1_000
MAX_TIMING_VALUES = 1_000

MetricKey = tuple[str, tuple[tuple[str, str], ...]]


def _build_key(stat: str, tags: dict[str, Any] | None) -> MetricKey:
    if not tags:
        return stat, ()
    # Tags travel in a typed API payload as strings, which is what every backend ends up storing.
    return stat, tuple(sorted((k, str(v)) for k, v in tags.items()))


def _skip_due_to_rate(rate: int | float) -> bool:
    return rate < 1 and random.random() > rate


class _ForwardingTimer(Timer):
    def __init__(self, logger: ForwardingStatsLogger, name: str | None, tags: dict[str, Any] | None):
        super().__init__()
        self._logger = logger
        self._name = name
        self._tags = tags

    def stop(self, send: bool = True) -> None:
        super().stop(send)
        if self._name and send and self.duration is not None:
            self._logger.timing(self._name, self.duration, tags=self._tags)


class ForwardingStatsLogger:
    """Aggregate metrics in memory and ship them to the supervisor in batches."""

    def __init__(self, comms: CommsDecoder):
        self._comms = comms
        self._lock = threading.Lock()
        self._counters: dict[MetricKey, int] = {}
        self._gauges: dict[MetricKey, tuple[float, bool]] = {}
        self._timings: dict[MetricKey, list[float]] = {}
        self._stop_flushing = threading.Event()
        self._flush_thread = threading.Thread(
            target=self._flush_periodically, name="stats-forwarder", daemon=True
        )
        self._flush_thread.start()

    def incr(
        self, stat: str, count: int = 1, rate: int | float = 1, *, tags: dict[str, Any] | None = None
    ) -> None:
        if _skip_due_to_rate(rate):
            return
        key = _build_key(stat, tags)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + count
            is_large = self._is_large()
        if is_large:
            self.flush()

    def decr(
        self, stat: str, count: int = 1, rate: int | float = 1, *, tags: dict[str, Any] | None = None
    ) -> None:
        self.incr(stat, -count, rate, tags=tags)

    def gauge(
        self,
        stat: str,
        value: float,
        rate: int | float = 1,
        delta: bool = False,
        *,
        tags: dict[str, Any] | None = None,
    ) -> None:
        if _skip_due_to_rate(rate):
            return
        key = _build_key(stat, tags)
        with self._lock:
            previous = self._gauges.get(key)
            if delta and previous is not None:
                # Fold the increment into what is already recorded; the result is still a delta
                # only if nothing absolute was recorded before it.
                value += previous[0]
                delta = previous[1]
            self._gauges[key] = (value, delta)
            is_large = self._is_large()
        if is_large:
            self.flush()

    def timing(self, stat: str, dt: DeltaType | None, *, tags: dict[str, Any] | None = None) -> None:
        if dt is None:
            return
        if isinstance(dt, datetime.timedelta):
            dt = dt.total_seconds() * 1000.0
        key = _build_key(stat, tags)
        with self._lock:
            values = self._timings.setdefault(key, [])
            values.append(float(dt))
            is_large = len(values) >= MAX_TIMING_VALUES or self._is_large()
        if is_large:
            self.flush()

    def timer(self, stat: str | None = None, *args, tags: dict[str, Any] | None = None, **kwargs) -> Timer:
        return _ForwardingTimer(self, stat, tags)

    def flush(self) -> None:
        """Send everything accumulated so far to the supervisor."""
        # Metrics must never break the task, so nothing in here is allowed to propagate.
        try:
            metrics = self._drain()
            if metrics:
                self._comms.send(ForwardMetrics(metrics=metrics))
        except Exception:
            log.warning("Could not forward metrics to the supervisor; dropping them", exc_info=True)

    def close(self) -> None:
        """Stop the periodic flush and send whatever is left; called once at process exit."""
        self._stop_flushing.set()
        self.flush()

    def _is_large(self) -> bool:
        return len(self._counters) + len(self._gauges) + len(self._timings) >= MAX_POINTS

    def _drain(self) -> list[ForwardedMetric]:
        with self._lock:
            metrics = [
                *(
                    ForwardedMetric(kind=MetricKind.COUNTER, name=name, tags=dict(tags) or None, value=value)
                    for (name, tags), value in self._counters.items()
                ),
                *(
                    ForwardedMetric(
                        kind=MetricKind.GAUGE, name=name, tags=dict(tags) or None, value=value, delta=delta
                    )
                    for (name, tags), (value, delta) in self._gauges.items()
                ),
                *(
                    ForwardedMetric(kind=MetricKind.TIMING, name=name, tags=dict(tags) or None, values=values)
                    for (name, tags), values in self._timings.items()
                ),
            ]
            self._counters.clear()
            self._gauges.clear()
            self._timings.clear()
        return metrics

    def _flush_periodically(self) -> None:
        while not self._stop_flushing.wait(FLUSH_INTERVAL_SECONDS):
            self.flush()
