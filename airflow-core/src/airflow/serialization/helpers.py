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
"""Serialized DAG and BaseOperator."""

from __future__ import annotations

import inspect
from typing import Any

from airflow._shared.secrets_masker import redact
from airflow.configuration import conf
from airflow.utils.module_loading import qualname


def serialize_template_field(template_field: Any, name: str) -> str | dict | list | int | float | bool | None:
    """
    Return a serializable representation of the templated field.

    The walk has two responsibilities:

    1. **Make the template_field JSON-encodable** — every container is rebuilt
       with primitive leaves (str/int/float/bool/None), tuples and sets are
       flattened to lists, and unsupported objects fall through to ``str()``
       so ``json.dumps`` never raises on the result.
    2. **Keep the output deterministic across parses** — callables are replaced
       with their qualified name (never the default ``<function ... at 0x...>``
       repr), dicts are key-sorted, and (frozen)sets are sorted by element so
       the same input always produces the same string.
    """

    def normalize_dict_key(key) -> str:
        """Normalize a dict key to a serialized string type."""
        # Serialized template_field keys must all be strings, not a mix of types, so that
        # downstream json.dumps(..., sort_keys=True) does not raise on mixed-type keys.
        return str(serialize_object(key))

    def serialize_object(obj):
        """Recursively rewrite ``obj`` into a JSON-encodable, hash-stable structure."""
        if obj is None or isinstance(obj, (str, int, float, bool)):
            return obj

        if isinstance(obj, dict):
            # Serialize keys/values first so each key is a string and the output is hash-stable,
            # then sort by the serialized key to prevent hash inconsistencies when dict ordering varies.
            serialized_pairs = [(normalize_dict_key(k), serialize_object(v)) for k, v in obj.items()]
            return dict(sorted(serialized_pairs, key=lambda kv: kv[0]))

        if isinstance(obj, (list, tuple)):
            return [serialize_object(item) for item in obj]

        if isinstance(obj, (set, frozenset)):
            # JSON has no set type → flatten to a list with deterministic ordering
            # so hash randomization on element types cannot shift cross-process iteration order.
            serialized_set = [serialize_object(e) for e in obj]
            return sorted(serialized_set, key=lambda x: (type(x).__name__, str(x)))

        # Use inspect.getattr_static to bypass any custom __getattr__ / metaclass magic
        if callable(inspect.getattr_static(obj, "serialize", None)):
            return serialize_object(obj.serialize())

        # Kubernetes client objects (V1Pod, V1Container, ...) expose their content via to_dict().
        # Scope the branch to the kubernetes namespace so unrelated user classes that happen to
        # define a to_dict() method fall through to str() instead of being treated as K8s payloads.
        if getattr(type(obj), "__module__", "").startswith(
            ("kubernetes.", "kubernetes_asyncio.")
        ) and callable(inspect.getattr_static(obj, "to_dict", None)):
            return serialize_object(obj.to_dict())

        if callable(obj):
            # Use qualified name; default repr embeds memory addresses, which would change the DAG hash on every parse
            return f"<callable {qualname(obj, True)}>"

        # A custom __str__ or __repr__ is treated as an intentional textual representation
        # supplied by the author and used as-is.
        if type(obj).__str__ is not object.__str__ or type(obj).__repr__ is not object.__repr__:
            return str(obj)

        # Otherwise fall back to a qualname marker. The default object repr is
        # `<ClassName object at 0x...>`, which embeds a memory address that flips per process
        # and would break DAG hash stability — use the class qualname instead.
        return f"<{qualname(type(obj), True)} object>"

    max_length = conf.getint("core", "max_templated_field_length")

    serialized = serialize_object(template_field)

    if len(str(serialized)) > max_length:
        rendered = redact(str(serialized), name)
        return (
            "Truncated. You can change this behaviour in [core]max_templated_field_length. "
            f"{str(rendered)[: max_length - 79]!r}... "
        )

    return serialized
