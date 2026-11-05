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

"""Throwaway check that the gross region dags parse and have the expected shape."""

from __future__ import annotations

import sys

from airflow.dag_processing.dagbag import BundleDagBag
from airflow.serialization.serialized_objects import DagSerialization

FOLDER = "/opt/airflow/airflow-core/tests/integration/perf/dags"

bag = BundleDagBag(
    dag_folder=f"{FOLDER}/gross",
    bundle_path=FOLDER,
    bundle_name="testing",
    safe_mode=False,
)

if bag.import_errors:
    for loc, err in bag.import_errors.items():
        print(f"IMPORT ERROR {loc}:\n{err}")
    sys.exit(1)

region_dags = sorted(d for d in bag.dag_ids if d.startswith("process_region"))
print(f"parsed {len(region_dags)} region dags")

dag = bag.dags["process_region1"]
print(f"tasks in process_region1: {len(dag.task_dict)}")
for task_id, task in dag.task_dict.items():
    print(f"  {task_id:55s} {type(task).__name__:25s} mapped={getattr(task, 'is_mapped', False)}")

DagSerialization.to_dict(dag)
print("serialization: ok")
