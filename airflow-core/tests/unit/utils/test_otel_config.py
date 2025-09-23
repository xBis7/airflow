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

from airflow.utils.otel_config import (
    _parse_kv_str_to_dict,
    load_metrics_config,
    load_traces_config,
)

from tests_common.test_utils.config import env_vars


def test_env_vars_snapshot():
    # pass a list of env vars and execute the method.
    # test the result
    pass


def test_config_validation():
    # load_otel_config()
    # once for traces and once for metrics
    # pass a list of env vars and validate.
    # Could parameterize it and pass invalid and valid configs.
    # traces - invalid endpoint
    # traces - invalid protocol
    # ...
    # metrics - invalid endpoint
    # metrics - invalid protocol
    # ...
    pass


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
        assert not config.headers_kv_str
        assert not config.resource_attributes_kv_str
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
        assert not config.headers_kv_str
        assert not config.resource_attributes_kv_str


def test_parsing_kv_str_configs():
    config_str = "service.name=my-service,service.version=1.0.0"

    config_dict = _parse_kv_str_to_dict(config_str)

    assert len(config_dict) == 2
    assert ("service.name", "my-service") in config_dict.items()
    assert ("service.version", "1.0.0") in config_dict.items()
