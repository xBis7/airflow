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

import os
import re
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.providers.apache.hive.version_compat import (
    AIRFLOW_VAR_NAME_FORMAT_MAPPING,
    BaseOperator,
    context_to_airflow_vars,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class HiveOperator(BaseOperator):
    """
    Executes hql code or hive script in a specific Hive database.

    :param hql: the hql to be executed. Note that you may also use
        a relative path from the dag file of a (template) hive
        script. (templated)
    :param hive_cli_conn_id: Reference to the
        :ref:`Hive CLI connection id <howto/connection:hive_cli>`. (templated)
    :param hiveconfs: if defined, these key value pairs will be passed
        to hive as ``-hiveconf "key"="value"``
    :param hiveconf_jinja_translate: when True, hiveconf-type templating
        ${var} gets translated into jinja-type templating {{ var }} and
        ${hiveconf:var} gets translated into jinja-type templating {{ var }}.
        Note that you may want to use this along with the
        ``DAG(user_defined_macros=myargs)`` parameter. View the DAG
        object documentation for more details.
    :param script_begin_tag: If defined, the operator will get rid of the
        part of the script before the first occurrence of `script_begin_tag`
    :param mapred_queue: queue used by the Hadoop CapacityScheduler. (templated)
    :param mapred_queue_priority: priority within CapacityScheduler queue.
        Possible settings include: VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW
    :param mapred_job_name: This name will appear in the jobtracker.
        This can make monitoring easier.
    :param hive_cli_params: parameters passed to hive CLO
    :param auth: optional authentication option passed for the Hive connection
    :param proxy_user: Run HQL code as this user.
    """

    template_fields: Sequence[str] = (
        "hql",
        "schema",
        "hive_cli_conn_id",
        "mapred_queue",
        "hiveconfs",
        "mapred_job_name",
        "mapred_queue_priority",
        "proxy_user",
    )
    template_ext: Sequence[str] = (
        ".hql",
        ".sql",
    )
    template_fields_renderers = {"hql": "hql"}
    ui_color = "#f0e4ec"

    def __init__(
        self,
        *,
        hql: str,
        hive_cli_conn_id: str = "hive_cli_default",
        schema: str = "default",
        hiveconfs: dict[Any, Any] | None = None,
        hiveconf_jinja_translate: bool = False,
        script_begin_tag: str | None = None,
        mapred_queue: str | None = None,
        mapred_queue_priority: str | None = None,
        mapred_job_name: str | None = None,
        hive_cli_params: str = "",
        auth: str | None = None,
        proxy_user: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.hql = hql
        self.hive_cli_conn_id = hive_cli_conn_id
        self.schema = schema
        self.hiveconfs = hiveconfs or {}
        self.hiveconf_jinja_translate = hiveconf_jinja_translate
        self.script_begin_tag = script_begin_tag
        self.mapred_queue = mapred_queue
        self.mapred_queue_priority = mapred_queue_priority
        self.mapred_job_name = mapred_job_name
        self.hive_cli_params = hive_cli_params
        self.auth = auth
        self.proxy_user = proxy_user
        job_name_template = conf.get_mandatory_value(
            "hive",
            "mapred_job_name_template",
            fallback="Airflow HiveOperator task for {hostname}.{dag_id}.{task_id}.{logical_date}",
        )
        self.mapred_job_name_template: str = job_name_template

    @cached_property
    def hook(self) -> HiveCliHook:
        """Get Hive cli hook."""
        return HiveCliHook(
            hive_cli_conn_id=self.hive_cli_conn_id,
            mapred_queue=self.mapred_queue,
            mapred_queue_priority=self.mapred_queue_priority,
            mapred_job_name=self.mapred_job_name,
            hive_cli_params=self.hive_cli_params,
            auth=self.auth,
            proxy_user=self.proxy_user,
        )

    def prepare_template(self) -> None:
        if self.hiveconf_jinja_translate:
            self.hql = re.sub(r"(\$\{(hiveconf:)?([ a-zA-Z0-9_]*)\})", r"{{ \g<3> }}", self.hql)
        if self.script_begin_tag and self.script_begin_tag in self.hql:
            self.hql = "\n".join(self.hql.split(self.script_begin_tag)[1:])

    def execute(self, context: Context) -> None:
        self.log.info("Executing: %s", self.hql)

        # set the mapred_job_name if it's not set with dag, task, execution time info
        if not self.mapred_job_name:
            ti = context["ti"]
            logical_date = context["logical_date"]
            if logical_date is None:
                raise RuntimeError("logical_date is None")
            hostname = ti.hostname or ""
            self.hook.mapred_job_name = self.mapred_job_name_template.format(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                logical_date=logical_date.isoformat(),
                hostname=hostname.split(".")[0],
            )

        if self.hiveconf_jinja_translate:
            self.hiveconfs = context_to_airflow_vars(context)
        else:
            self.hiveconfs.update(context_to_airflow_vars(context))

        self.log.info("Passing HiveConf: %s", self.hiveconfs)
        self.hook.run_cli(hql=self.hql, schema=self.schema, hive_conf=self.hiveconfs)

    def dry_run(self) -> None:
        # Reset airflow environment variables to prevent
        # existing env vars from impacting behavior.
        self.clear_airflow_vars()

        self.hook.test_hql(hql=self.hql)

    def on_kill(self) -> None:
        if self.hook:
            self.hook.kill()

    def clear_airflow_vars(self) -> None:
        """Reset airflow environment variables to prevent existing ones from impacting behavior."""
        blank_env_vars = {value["env_var_format"]: "" for value in AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()}
        os.environ.update(blank_env_vars)
