#!/usr/bin/env bash
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

# Runs inside the breeze container; called from scripts/in_container/check_environment.sh.
set -euo pipefail

UX_DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${FILES_DIR:-/files}/ux_demo_seed.log"

# The terminal multiplexer takes over the screen right after startup, so keep a copy of the output.
AIRFLOW__LOGGING__LOGGING_LEVEL=WARNING python "${UX_DEMO_DIR}/seed_demo_data.py" 2>&1 | tee "${LOG_FILE}"
