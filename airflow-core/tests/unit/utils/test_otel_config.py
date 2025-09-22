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

from airflow.utils.otel_config import load_metrics_config, load_traces_config

from tests_common.test_utils.config import env_vars


def test_metrics_validation():
    with pytest.raises(OSError) as endpointExc:
        load_metrics_config()

    assert "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT" in str(endpointExc.value)

    url = "http://localhost:4318/v1/metrics"

    with env_vars({"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": url}):
        config = load_metrics_config()

        assert config.endpoint == url
        # Default values.
        assert config.service_name == "Airflow"
        assert config.protocol == "grpc"
        assert not config.headers_raw
        assert not config.resource_attributes_raw
        # Check that the value is an int and not str.
        assert config.interval != "60000"
        assert config.interval == 60000


def test_traces_validation():
    with pytest.raises(OSError) as endpointExc:
        load_traces_config()

    assert "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" in str(endpointExc.value)

    url = "http://localhost:4318/v1/traces"

    with env_vars({"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": url}):
        config = load_traces_config()

        assert config.endpoint == url
        # Default values.
        assert config.service_name == "Airflow"
        assert config.protocol == "grpc"
        assert not config.headers_raw
        assert not config.resource_attributes_raw
