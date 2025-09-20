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
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache

log = logging.getLogger(__name__)


def _parse_headers(headers_str: str) -> dict[str, str]:
    """Parse headers from string."""
    headers = {}
    if headers_str:
        for pair in headers_str.split(","):
            if "=" in pair:
                k, v = pair.split("=", 1)
                headers[k.strip()] = v.strip()
    return headers


def _parse_attributes(attributes_str: str) -> dict[str, str]:
    """Parse attributes from string."""
    attrs = {}
    if attributes_str:
        for pair in attributes_str.split(","):
            if "=" in pair:
                k, v = pair.split("=", 1)
                attrs[k.strip()] = v.strip()
    return attrs


class OtelKind(str, Enum):
    """Enum with the different kinds of config variables."""

    TRACES = "traces"
    METRICS = "metrics"


@dataclass(frozen=True)
class OtelConfig:
    """Immutable class for holding and validating OTel config environment variables."""

    kind: OtelKind  # traces | metrics
    endpoint: str  # resolved endpoint for this kind
    protocol: str  # "grpc" or "http/protobuf"
    exporter: str  # OTEL_TRACES_EXPORTER | OTEL_METRICS_EXPORTER
    service_name: str  # default "Airflow"
    headers_raw: str
    headers: dict[str, str]
    resource_attributes_raw: str
    resource_attributes: dict[str, str]
    interval: float

    def __post_init__(self):
        if not self.endpoint:
            specific = (
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
                if self.kind == OtelKind.TRACES
                else "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"
            )
        else:
            specific = ""
        if not self.endpoint:
            raise OSError(
                f"Missing required environment variable: {specific or 'OTEL_EXPORTER_OTLP_ENDPOINT'}"
            )
        if self.protocol not in ("grpc", "http/protobuf"):
            raise ValueError(f"Invalid OTEL_EXPORTER_OTLP_PROTOCOL: {self.protocol}")

        if self.protocol == "http/protobuf":
            suffix = "/v1/traces" if self.kind == OtelKind.TRACES else "/v1/metrics"
            if not self.endpoint.rstrip("/").endswith(suffix):
                # Not fatal, but commonly misconfigured.
                log.error("Misconfigured OTEL_EXPORTER_OTLP_ENDPOINT: ", self.endpoint)
                pass
                pass


def _env_snapshot(kind: OtelKind) -> tuple[str | None, ...]:
    """
    Return a tuple of the relevant env values.

    If any of these change, the snapshot changes, invalidating the cache entry.
    """
    # common
    common = (
        os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
        os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL"),
        os.getenv("OTEL_SERVICE_NAME"),
        os.getenv("OTEL_EXPORTER_OTLP_HEADERS"),
        os.getenv("OTEL_RESOURCE_ATTRIBUTES"),
    )
    if kind == OtelKind.TRACES:
        specific = (
            os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
            os.getenv("OTEL_TRACES_EXPORTER"),
        )
    else:
        specific = (
            os.getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"),
            os.getenv("OTEL_METRICS_EXPORTER"),
        )
    return (kind.value, *specific, *common)


@lru_cache
def load_otel_config(kind: OtelKind, snapshot: tuple | None = None) -> OtelConfig:
    """
    Read and validate OTel config env vars once per unique snapshot.

    `_env_snapshot()` is passed as the argument whenever this function is called.
    If the env changes, the snapshot changes, so this recomputes.
    """
    protocol = os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
    service_name = os.getenv("OTEL_SERVICE_NAME", "Airflow")
    headers_raw = os.getenv("OTEL_EXPORTER_OTLP_HEADERS", "")
    resource_attributes_raw = os.getenv("OTEL_RESOURCE_ATTRIBUTES", "")

    if kind == OtelKind.TRACES:
        endpoint = (
            os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or ""
        )
        exporter = os.getenv("OTEL_TRACES_EXPORTER", "otlp")
        interval = 0
    else:
        endpoint = (
            os.getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or ""
        )
        exporter = os.getenv("OTEL_METRICS_EXPORTER", "otlp")
        interval = int(os.getenv("OTEL_METRIC_EXPORT_INTERVAL", "60000"))

    return OtelConfig(
        kind=kind,
        endpoint=endpoint,
        protocol=protocol,
        exporter=exporter,
        service_name=service_name,
        headers_raw=headers_raw,
        headers=_parse_headers(headers_raw),
        resource_attributes_raw=resource_attributes_raw,
        resource_attributes=_parse_attributes(resource_attributes_raw),
        interval=interval,
    )


# Convenience wrappers so callers don't deal with enums/snapshots.
def load_traces_config() -> OtelConfig:
    return load_otel_config(OtelKind.TRACES, _env_snapshot(OtelKind.TRACES))


def load_metrics_config() -> OtelConfig:
    return load_otel_config(OtelKind.METRICS, _env_snapshot(OtelKind.METRICS))


def invalidate_otel_config_cache() -> None:
    """Manually force a refresh."""
    load_otel_config.cache_clear()
