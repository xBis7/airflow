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
import logging
import os
import subprocess
import time
import uuid

import pytest

from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook
from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from airflow.providers.apache.kafka.operators.produce import acked
from airflow.sdk import Connection

from tests_common.test_utils.integration_setup import serialize_and_get_dags

log = logging.getLogger(__name__)

# Shared connection used by both producer and consumer in this test. Consumer-only
# keys (group.id, enable.auto.commit) are required for Consumer construction; the
# producer logs CONFWARN about them being ignored — informational, harmless.
client_config = {
    "bootstrap.servers": "broker:29092",
    "group.id": "kafka-listener-integration-test",
    "enable.auto.commit": False,
    # auto.offset.reset is irrelevant when the consumer subscribes BEFORE the
    # producer publishes (current pattern) — librdkafka places the consumer at
    # the end of the log on join. Set it to ``earliest`` anyway as a safety net
    # in case the pattern flips later.
    "auto.offset.reset": "earliest",
}


def _wait_for_assignment(consumer, timeout: float = 10.0) -> None:
    """Block until the consumer has been assigned partitions by the broker."""
    deadline = time.monotonic() + timeout
    while not consumer.assignment():
        consumer.poll(0.5)
        if time.monotonic() > deadline:
            raise TimeoutError("Kafka consumer did not receive partition assignment in time")


@pytest.mark.integration("kafka")
@pytest.mark.backend("postgres")
class TestEventListener:
    test_dir = os.path.dirname(os.path.abspath(__file__))
    dag_folder = os.path.join(test_dir, "dags")

    KAFKA_CONFIG_ID = "kafka_default"
    # Unique topic per test class run. Stable-named topics race against Kafka's
    # async delete: on a re-run, the previous teardown's delete may still be in
    # flight, and the broker rejects the new CreateTopics with KafkaError UNKNOWN.
    # A per-run name eliminates that window entirely.
    TOPIC = f"airflow.events.itest.{uuid.uuid4().hex[:8]}"

    @classmethod
    def setup_class(cls):
        # The pytest plugin strips AIRFLOW__*__* env vars (including the JWT secret set
        # by Breeze). Both the scheduler and api-server subprocesses must share the same
        # secret; otherwise each generates its own random key and token verification fails.
        os.environ["AIRFLOW__API_AUTH__JWT_SECRET"] = "test-secret-key-for-testing"
        os.environ["AIRFLOW__API_AUTH__JWT_ISSUER"] = "airflow"

        os.environ["AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR"] = "False"
        os.environ["AIRFLOW__SCHEDULER__PROCESSOR_POLL_INTERVAL"] = "2"

        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{cls.dag_folder}"
        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        # os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = "/dev/null"
        os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"

        # Kafka listener configs.
        os.environ["AIRFLOW__KAFKA_LISTENER__ENABLED"] = "True"
        os.environ["AIRFLOW__KAFKA_LISTENER__BOOTSTRAP_SERVERS"] = "broker:29092"
        os.environ["AIRFLOW__KAFKA_LISTENER__TOPIC"] = cls.TOPIC
        os.environ["AIRFLOW__KAFKA_LISTENER__SOURCE"] = "dev-breeze"

        # 1. Create the connection FIRST — via the AIRFLOW_CONN_* env var so it's
        # visible to this test process, to KafkaAdminClientHook below, and to any
        # scheduler / api-server subprocesses the test starts later. The env-var
        # path goes through EnvironmentVariablesSecretsBackend which is in every
        # secrets-backend chain regardless of process context.
        kafka_default_conn = Connection(
            conn_id=cls.KAFKA_CONFIG_ID,
            conn_type="kafka",
            extra=json.dumps(client_config),
        )
        os.environ["AIRFLOW_CONN_KAFKA_DEFAULT"] = kafka_default_conn.as_json()

        # 2. Reset and migrate the DB.
        reset_command = ["airflow", "db", "reset", "--yes"]
        subprocess.run(reset_command, check=True, env=os.environ.copy())

        migrate_command = ["airflow", "db", "migrate"]
        subprocess.run(migrate_command, check=True, env=os.environ.copy())

        cls.dags = serialize_and_get_dags(dag_folder=cls.dag_folder)

        # 3. Create the topic — uses kafka_default created above.
        cls._admin = KafkaAdminClientHook(kafka_config_id=cls.KAFKA_CONFIG_ID)
        # The topic sequence follows the structure '(name, partition, replication)'.
        cls._admin.create_topic([(cls.TOPIC, 1, 1)])

    @classmethod
    def teardown_class(cls):
        # Best-effort cleanup so the next run gets a fresh topic with no leftover
        # messages or committed offsets.
        try:
            cls._admin.delete_topic([cls.TOPIC])
            time.sleep(2)  # let the broker finish the async delete
        except Exception as exc:
            log.warning("teardown: failed to delete topic %r: %s", cls.TOPIC, exc)

    @pytest.mark.execution_timeout(90)
    def test_produce_event_messages(self):
        # 1. Bring up the consumer FIRST so it's joined to the group and positioned
        # at the current end of the log before any message is produced. This means
        # we don't depend on auto.offset.reset semantics or committed-offset state.
        consumer = KafkaConsumerHook(topics=[self.TOPIC], kafka_config_id=self.KAFKA_CONFIG_ID).get_consumer()
        _wait_for_assignment(consumer)

        # 2. Now produce — the message lands after the consumer's starting position.
        producer = KafkaProducerHook(kafka_config_id=self.KAFKA_CONFIG_ID).get_producer()
        producer.produce(self.TOPIC, key=b"p1", value=b"p2", on_delivery=acked)
        pending = producer.flush(10)
        assert pending == 0, f"producer still had {pending} messages pending after flush"

        # 3. Consume.
        try:
            msgs = consumer.consume(num_messages=1, timeout=10.0)
        finally:
            consumer.close()

        print(f"x: {msgs}")

        assert msgs, "no message received from topic within 10s"
        assert msgs[0].value() == b"p2"
        assert msgs[0].key() == b"p1"
