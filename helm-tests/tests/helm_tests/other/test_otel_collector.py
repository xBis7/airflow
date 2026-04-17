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

import jmespath
import pytest
import yaml
from chart_utils.helm_template_generator import render_chart


class TestOtelCollector:
    """Tests the OTel Collector deployment, service, and ConfigMap."""

    def test_default_renders_deployment_service_and_configmap(self):
        docs = render_chart(
            show_only=[
                "templates/otel-collector/otel-collector-deployment.yaml",
                "templates/otel-collector/otel-collector-service.yaml",
                "templates/configmaps/otel-collector-configmap.yaml",
            ],
        )
        assert len(docs) == 3
        assert {doc["kind"] for doc in docs} == {"Deployment", "Service", "ConfigMap"}

    def test_disabled_renders_nothing(self):
        docs = render_chart(
            values={"otelCollector": {"enabled": False}},
            show_only=[
                "templates/otel-collector/otel-collector-deployment.yaml",
                "templates/otel-collector/otel-collector-service.yaml",
                "templates/configmaps/otel-collector-configmap.yaml",
            ],
        )
        assert docs == []

    def test_fails_when_no_pipeline_enabled_and_no_override(self):
        with pytest.raises(Exception, match="otelCollector.enabled is true"):
            render_chart(
                values={
                    "otelCollector": {
                        "enabled": True,
                        "tracesEnabled": False,
                        "metricsEnabled": False,
                    }
                },
                show_only=["templates/configmaps/otel-collector-configmap.yaml"],
            )

    def test_config_override_renders_verbatim(self):
        override = "extensions: {}\nservice:\n  extensions: []\n"
        docs = render_chart(
            values={
                "otelCollector": {
                    "enabled": True,
                    "tracesEnabled": False,
                    "metricsEnabled": False,
                    "configOverride": override,
                }
            },
            show_only=["templates/configmaps/otel-collector-configmap.yaml"],
        )
        rendered = jmespath.search('data."config.yml"', docs[0])
        parsed = yaml.safe_load(rendered)
        assert parsed == {"extensions": {}, "service": {"extensions": []}}

    @pytest.mark.parametrize(
        ("traces_enabled", "metrics_enabled", "expected_pipelines"),
        [
            (True, False, {"traces"}),
            (False, True, {"metrics"}),
            (True, True, {"traces", "metrics"}),
        ],
    )
    def test_pipelines_are_conditional(self, traces_enabled, metrics_enabled, expected_pipelines):
        docs = render_chart(
            values={
                "otelCollector": {
                    "enabled": True,
                    "tracesEnabled": traces_enabled,
                    "metricsEnabled": metrics_enabled,
                }
            },
            show_only=["templates/configmaps/otel-collector-configmap.yaml"],
        )
        rendered = jmespath.search('data."config.yml"', docs[0])
        parsed = yaml.safe_load(rendered)
        assert set(parsed["service"]["pipelines"]) == expected_pipelines
        # prometheus exporter should only be present when metrics are enabled
        assert ("prometheus" in parsed["exporters"]) is metrics_enabled

    def test_metrics_port_only_when_metrics_enabled(self):
        docs = render_chart(
            values={"otelCollector": {"metricsEnabled": False}},
            show_only=[
                "templates/otel-collector/otel-collector-deployment.yaml",
                "templates/otel-collector/otel-collector-service.yaml",
            ],
        )
        deploy_ports = jmespath.search("spec.template.spec.containers[0].ports[*].name", docs[0])
        svc_ports = jmespath.search("spec.ports[*].name", docs[1])
        assert "metrics" not in deploy_ports
        assert "metrics" not in svc_ports

        docs = render_chart(
            values={"otelCollector": {"metricsEnabled": True}},
            show_only=[
                "templates/otel-collector/otel-collector-deployment.yaml",
                "templates/otel-collector/otel-collector-service.yaml",
            ],
        )
        deploy_ports = jmespath.search("spec.template.spec.containers[0].ports[*].name", docs[0])
        svc_ports = jmespath.search("spec.ports[*].name", docs[1])
        assert "metrics" in deploy_ports
        assert "metrics" in svc_ports

    def test_airflow_config_gates_otel_on_enabled(self):
        # When enabled=false, even with metricsEnabled=true, otel_on must be False
        # and statsd must remain enabled.
        docs = render_chart(
            values={"otelCollector": {"enabled": False, "metricsEnabled": True, "tracesEnabled": True}},
            show_only=["templates/configmaps/configmap.yaml"],
        )
        airflow_cfg = jmespath.search('data."airflow.cfg"', docs[0])
        # Simple substring checks since the cfg is INI-formatted text.
        assert "otel_on = False" in airflow_cfg
        assert "statsd_on = True" in airflow_cfg

    def test_airflow_config_disables_statsd_when_metrics_enabled(self):
        docs = render_chart(
            values={"otelCollector": {"enabled": True, "metricsEnabled": True}},
            show_only=["templates/configmaps/configmap.yaml"],
        )
        airflow_cfg = jmespath.search('data."airflow.cfg"', docs[0])
        assert "statsd_on = False" in airflow_cfg
        assert "otel_on = True" in airflow_cfg

    def test_otel_port_uses_configured_port(self):
        docs = render_chart(
            values={
                "otelCollector": {"enabled": True, "metricsEnabled": True},
                "ports": {"otelCollectorOtlpHttp": 14318},
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )
        airflow_cfg = jmespath.search('data."airflow.cfg"', docs[0])
        assert "otel_port = 14318" in airflow_cfg

    def test_env_vars_use_otlp_exporter_and_custom_interval(self):
        docs = render_chart(
            values={
                "otelCollector": {
                    "enabled": True,
                    "tracesEnabled": True,
                    "metricsEnabled": True,
                    "metricExportInterval": 60000,
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        envs = jmespath.search("spec.template.spec.containers[0].env", docs[0])
        env_map = {e["name"]: e.get("value") for e in envs if "value" in e}
        assert env_map.get("OTEL_TRACES_EXPORTER") == "otlp"
        assert env_map.get("OTEL_METRIC_EXPORT_INTERVAL") == "60000"
        assert env_map.get("OTEL_EXPORTER_OTLP_PROTOCOL") == "http/protobuf"

    def test_env_vars_absent_when_collector_disabled(self):
        docs = render_chart(
            values={"otelCollector": {"enabled": False}},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        envs = jmespath.search("spec.template.spec.containers[0].env", docs[0]) or []
        env_names = {e["name"] for e in envs}
        assert not any(name.startswith("OTEL_") for name in env_names)

    def test_configurable_probes(self):
        docs = render_chart(
            values={
                "otelCollector": {
                    "livenessProbe": {
                        "initialDelaySeconds": 5,
                        "periodSeconds": 20,
                        "timeoutSeconds": 2,
                        "failureThreshold": 6,
                    },
                    "readinessProbe": {
                        "initialDelaySeconds": 7,
                        "periodSeconds": 25,
                        "timeoutSeconds": 3,
                        "failureThreshold": 8,
                    },
                }
            },
            show_only=["templates/otel-collector/otel-collector-deployment.yaml"],
        )
        container = jmespath.search("spec.template.spec.containers[0]", docs[0])
        assert container["livenessProbe"]["initialDelaySeconds"] == 5
        assert container["livenessProbe"]["periodSeconds"] == 20
        assert container["livenessProbe"]["timeoutSeconds"] == 2
        assert container["livenessProbe"]["failureThreshold"] == 6
        assert container["readinessProbe"]["initialDelaySeconds"] == 7
        assert container["readinessProbe"]["periodSeconds"] == 25

    def test_custom_command_and_args(self):
        docs = render_chart(
            values={
                "otelCollector": {
                    "command": ["/bin/otelcol-custom"],
                    "args": ["--config=/custom/path.yaml", "--feature-gates=foo"],
                }
            },
            show_only=["templates/otel-collector/otel-collector-deployment.yaml"],
        )
        container = jmespath.search("spec.template.spec.containers[0]", docs[0])
        assert container["command"] == ["/bin/otelcol-custom"]
        assert container["args"] == ["--config=/custom/path.yaml", "--feature-gates=foo"]

    def test_configmap_annotations(self):
        docs = render_chart(
            values={"otelCollector": {"configMapAnnotations": {"foo": "bar"}}},
            show_only=["templates/configmaps/otel-collector-configmap.yaml"],
        )
        assert jmespath.search("metadata.annotations", docs[0]) == {"foo": "bar"}

    def test_service_annotations(self):
        docs = render_chart(
            values={"otelCollector": {"service": {"annotations": {"foo": "bar"}}}},
            show_only=["templates/otel-collector/otel-collector-service.yaml"],
        )
        assert jmespath.search("metadata.annotations", docs[0]) == {"foo": "bar"}
