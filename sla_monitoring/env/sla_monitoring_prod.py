"""
Feed SLA monitoring DAG.

Resolves active feed_control_config in catalog-api, evaluates SLA locally (Airflow),
then persists results via POST record-evaluations (arrival_sla + structural_validation_deadline).

Requires:
  - catalog-api with feed control migrations applied
  - At least one active feed_control_config for the tenant
  - Keycloak service account secret (bh_kc_secret_url) for catalog API auth

The empty values below are placeholders: the sync-sla-monitoring-dag GitHub Action
replaces them with the SLA_MONITOR_PARAMS_JSON values at deploy time, so this file is a
template and will not run as-is. The os.environ and Variable.get expressions are never
injected; they resolve in the Airflow container.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.sla_monitoring_task import SLAMonitoringTask

default_databricks_warehouse_id = ""
default_control_catalog = ""
default_control_schema = ""

_SLA_MONITOR_PARAMS = {
    "tenant_id": 0,
    "bh_environment_id": 0,
    "bh_environment_name": "",

    "sla_env_yaml_blob_prefix": "",
    "sla_env_yaml_environment_file": "",

    "bh_kc_secret_url": "",
    "cloud_provider": "",
    "airflow_connection_id": "",

    "catalog_url": os.environ.get("CATALOG_URL") or "http://catalog-app:8011/api/v1",
    "keycloak_url": os.environ.get("KEYCLOAK_URL") or "http://keycloak:8080",

    "databricks_warehouse_id": Variable.get(
        "default_databricks_warehouse_id",
        default_var=default_databricks_warehouse_id,
    ),

    "control_catalog": Variable.get(
        "default_control_catalog",
        default_var=default_control_catalog,
    ),

    "control_schema": Variable.get(
        "default_control_schema",
        default_var=default_control_schema,
    ),

    "facts_source": "",
    "fail_on_error": False,
}

common_task = CommonTask(dag_id="sla_monitoring", dag_params={})
sla_task = SLAMonitoringTask(dag_id="sla_monitoring", dag_params={})

default_args = {
    "owner": "bh",
    "start_date": datetime.now() - timedelta(days=1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="sla_monitoring",
    default_args=default_args,
    # schedule=None,
    schedule='*/5 * * * *', # testing
    catchup=False,
    tags=["sla", "monitoring", "feed-control"],
    doc_md=__doc__,
) as dag:

    start_flow_task = PythonOperator(
        task_id="start_flow_task",
        python_callable=common_task.start_dag_task,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
        params={
            "flow_id": 0,
            "flow_name": "SLA Monitoring",
            "flow_key": "sla_monitoring",
            "flow_type": "OBSERVABILITY",
            "flow_status": "In Progress",
            **_SLA_MONITOR_PARAMS,
        },
    )

    evaluate_feed_sla_controls = PythonOperator(
        task_id="evaluate_feed_sla_controls",
        pre_execute=common_task.pre_execute_callback,
        python_callable=sla_task.evaluate_feed_sla_controls,
        params=_SLA_MONITOR_PARAMS,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    end_flow_task = PythonOperator(
        task_id="end_flow_task",
        pre_execute=common_task.pre_execute_callback,
        python_callable=common_task.end_dag_task,
        on_success_callback=common_task.flow_success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    start_flow_task >> evaluate_feed_sla_controls >> end_flow_task
