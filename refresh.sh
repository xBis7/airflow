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

set -euo pipefail

venv_path=$1 # /Users/xbis/utils/airflow-venv

# --------------------------------------------------------------------
# Copy the Celery files under 'airflow/files'.
# --------------------------------------------------------------------

declare -A files_map

files_map["task.py"]="celery/app/task.py"
files_map["base.py"]="celery/app/base.py"
files_map["connection.py"]="kombu/connection.py"

for filename in "${!files_map[@]}"; do
    file="${files_map[$filename]}"

    src="$venv_path/lib/python3.12/site-packages/$file"

    dest_dir="files"
    dest_path="${dest_dir}/$filename"

    mkdir -p "${dest_dir}"
    cp "${src}" "${dest_path}"

    echo "[refresh] File: $file → name: $filename"
    echo "[refresh] Copied: ${src} → ${dest_path}"
done

# --------------------------------------------------------------------
# Create airflow/files/setup.sh and make it executable
# --------------------------------------------------------------------
setup_script="${dest_dir}/setup.sh"

cat >"${setup_script}" <<'EOF'
#!/usr/bin/env bash
#
# setup.sh – executed inside the container, under breeze
# --------------------------------------------------------------------
set -euo pipefail

echo "[setup] Installing debugger …"
pip install "pydevd_pycharm~=242.23339.19"

declare -A files_map

files_map["task.py"]="celery/app/task.py"
files_map["base.py"]="celery/app/base.py"
files_map["connection.py"]="kombu/connection.py"

container_path="../../usr/local/lib/python3.12/site-packages"

for filename in "${!files_map[@]}"; do
    rel_path="${files_map[$filename]}"

    target="$container_path/$rel_path"

    if [ -f "${target}" ]; then
        cp "${target}" "${target}.bak"
    fi

    cp -f /files/"$filename" "${target}"
    echo "[setup] $filename successfully moved under: ${target}"
done

EOF

chmod u+x "${setup_script}"
echo "[refresh] Created and chmod +x: ${setup_script}"

echo "[refresh] Finished."
