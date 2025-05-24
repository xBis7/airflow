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

import redis.exceptions
from kombu.exceptions import OperationalError, TimeoutError
from redis.sentinel import MasterNotFoundError

from airflow.exceptions import AirflowTaskTimeout

CONFIG_OVERRIDES = {
    "broker_url": "sentinel://sentinel-1:26379;sentinel://sentinel-2:26379;sentinel://sentinel-3:26379/0",
    "result_backend": "db+postgresql://postgres:airflow@postgres/airflow",
    "broker_transport_options": {"master_name": "test-cluster"},
    "task_default_queue": "default",
}

CONFIG_WITH_RETRY_POLICY = {
    **CONFIG_OVERRIDES,
    "task_publish_retry": True,
    "task_publish_retry_policy": {
        "max_retries": 20,
        "interval_start": 10,
        "interval_step": 2,
        "interval_max": 20,
        "retry_errors": (
            Exception,
            redis.exceptions.ConnectionError,
            OperationalError,
            TimeoutError,
            AirflowTaskTimeout,
            MasterNotFoundError,
        ),
    },
}
