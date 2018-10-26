# -*- coding: utf-8 -*-
#
# Copyright (c) John Lewis & Partners
#
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This contains a plugin that can be used to branch on bigquery results
"""

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import BaseOperator, SkipMixin
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

class BranchBQOperator(BaseOperator, SkipMixin):
    """
    This operator is used to branch on the result of some bigquery sql.
    The sql should return one row, and all columns are evaluated as either true or false
    If all columns return something that is converted to true, then the pass_task is followed
    If all columns return something that is converted to false, then the fail_task is followed

    Note that Python bool casting evals the following as ``False``:
    * ``False``
    * ``0``
    * Empty string (``""``)
    * Empty list (``[]``)
    * Empty dictionary or set (``{}``)

    In this example, if my_dataset.my_table contains rows, then pass_task is followed
    Otherwise fail_task is followed

    CHECK_LAST_UPDATE_DATE_FOR_TABLES = BranchBQOperator(
        task_id='check_last_update_for_tables',
        sql='select count(*) from my_dataset.my_table,
        use_legacy_sql=False,
        bigquery_conn_id='bigquery_default',
        pass_task='pass_task',
        fail_task='fail_task',
        dag=dag)

    PASS_TASK = dummy_operator.DummyOperator(
        task_id='pass_task',
        dag=dag)

    FAIL_TASK = dummy_operator.DummyOperator(
        task_id='start',
        dag=dag)


    :param bigquery_conn_id: The connection ID to use when
        connecting to BigQuery.
    :type bigquery_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide delegation enabled.
    :type delegate_to: string
    :param sql: The sql to run. This should only return one row
    :type sql: string
    :param pass_task: The task to run next if all sql results are true
    :type pass_task: string
    :param fail_task: The task to run next if one of sql results are not true
    :type fail_task: string
    :param use_legacy_sql: Whether to use legacy SQL (true)
        or standard SQL (false).
    :type use_legacy_sql: bool
    """
    template_fields = ('sql',)
    template_ext = ('.sql', )

    @apply_defaults
    def __init__(
            self,
            sql=None,
            pass_task=None,
            fail_task=None,
            use_legacy_sql=True,
            bigquery_conn_id='bigquery_default',
            delegate_to=None,
            *args,
            **kwargs):
        super(BranchBQOperator, self).__init__(*args, **kwargs)
        self.bigquery_conn_id = bigquery_conn_id
        self.sql = sql
        self.pass_task = pass_task
        self.fail_task = fail_task
        self.delegate_to = delegate_to
        self.use_legacy_sql = use_legacy_sql

    def execute(self, context=None):
        self.log.info('Executing SQL check: %s', self.sql)
        hook = BigQueryHook(
            bigquery_conn_id=self.bigquery_conn_id,
            use_legacy_sql=self.use_legacy_sql,
            delegate_to=self.delegate_to)
        records = hook.get_first(self.sql)
        self.log.info('Record: %s', records)
        branch_to_follow = self.pass_task
        if not records:
            self.log.info('The query returned None')
            branch_to_follow = self.fail_task
        elif not all([bool(r) for r in records]):
            exceptstr = 'Test failed.\nQuery:\n{q}\nResults:\n{r!s}'
            self.log.info(exceptstr.format(q=self.sql, r=records))
            branch_to_follow = self.fail_task
        downstream_tasks = context['task'].downstream_list
        self.log.info('Following branch %s', branch_to_follow)
        self.log.info('Downstream task_ids %s', downstream_tasks)
        skip_tasks = [t for t in downstream_tasks if t.task_id != branch_to_follow]
        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, skip_tasks)

class BigQueryBranchCustomPlugin(AirflowPlugin):
    """
    define the plugin class
    """
    name = 'BigQueryBranchCustomPlugin'
    operators = [BranchBQOperator]
