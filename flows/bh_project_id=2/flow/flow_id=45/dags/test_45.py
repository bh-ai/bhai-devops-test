
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask

common_task = CommonTask(dag_id='test_45', dag_params={})
lineage_task = LineageTask(dag_id='test_45', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='test_45',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=[]
) as dag:


    from airflow.operators.python import PythonOperator
    start_flow_task = PythonOperator(
        task_id='start_flow_task',
        python_callable=common_task.start_dag_task,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
        params = {
            'flow_id': 45,
            'flow_name': 'Test',
            'flow_key': 'test',
            'bh_project_id': 2,
            'project_name': 'flow-service-project',
            'flow_tags': [],
            'flow_type': 'INGESTION',
            'tenant_id': 1,
            'flow_status': 'In Progress',
        }
    )

    from airflow.operators.python import PythonOperator
    from airflow.providers.databricks.hooks.databricks import DatabricksHook

    def create_databricks_cluster_create_compute_4214f3af1(**context):
        from airflow_plugins.cloud_factory import CloudFactory
        hook = DatabricksHook(databricks_conn_id='databricks_default')
        conn = hook.get_conn()
        workspace_url = (conn.host or '').rstrip('/')
        token = conn.password
        user_account = conn.login
        if not user_account:
            try:
                import requests as _bh_rq
                _bh_me = _bh_rq.get(
                    workspace_url + '/api/2.0/preview/scim/v2/Me',
                    headers={'Authorization': 'Bearer ' + token},
                    timeout=10,
                )
                if _bh_me.status_code == 200:
                    _bh_d = _bh_me.json()
                    user_account = _bh_d.get('userName') or (_bh_d.get('emails') or [{}])[0].get('value')
            except Exception:
                pass
        user_account = user_account or 'unknown'
        if not workspace_url or not token:
            raise ValueError("Databricks connection must have host and password (token)")
        factory = CloudFactory("databricks", databricks_workspace_url=workspace_url, databricks_token=token)
        compute = factory.get_compute(compute_type="databricks")
        payload = (
            {
                "cluster_name": "dbricks_test_cluster",
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "Standard_D4s_v3",
                "num_workers": 0,
                "autoscale": None,
                "driver_node_type_id": None,
                "runtime_engine": None,
                "data_security_mode": "SINGLE_USER",
                "single_user_name": "sathish@bighammer.ai",
                "policy_id": None,
                "apply_policy_default_values": True,
                "idempotency_token": None,
                "aws_attributes": None,
                "azure_attributes": None,
                "gcp_attributes": None,
                "single_node": True,
                "autotermination_minutes": 15,
                "enable_elastic_disk": True,
                "spark_conf": {},
                "spark_env_vars": {
                    "SECRET_MANAGER_PROVIDER": "databricks"
                },
                "custom_tags": {},
                "init_scripts": [
                    "/Workspace/Shared/bh-dev-utils/scripts/bh_databricks_grpc_server.sh"
                ],
                "libraries": [],
                "databricks_region": None,
                "bh_tags": []
            }
        )
        cluster_id = compute.create_compute(
            payload,
            compute_name=payload.get("cluster_name"),
            run_async=False,
        )
        if not cluster_id:
            raise ValueError("create_compute did not return cluster_id")

        num_workers = payload.get("num_workers", 0)
        context["ti"].xcom_push(key="bh_audit_metadata", value={
            "databricks_cluster_id": cluster_id,
            "databricks_cluster_size": num_workers,
            "databricks_user_account": user_account,
            "ingestion_group_id": None,
            "flow_id": 45
        })
        return cluster_id

    create_compute_4214f3af1 = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='create_compute_4214f3af1',
        python_callable=create_databricks_cluster_create_compute_4214f3af1,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    from airflow.operators.python import PythonOperator
    from airflow_plugins.cloud_factory import CloudFactory
    import logging
    logger = logging.getLogger(__name__)

    def terminate_databricks_resources(**context):
        ti = context["ti"]
        compute_id = ti.xcom_pull(task_ids="create_compute_4214f3af1", key="return_value")
        if not compute_id or (isinstance(compute_id, str) and "{" in compute_id):
            params = context.get("params") or {}
            compute_id = params.get("compute_id")
        if not compute_id or (isinstance(compute_id, str) and "{" in compute_id):
            logger.warning("No compute_id from XCom task create_compute_4214f3af1 or params; skipping terminate")
            return
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('databricks_default')
        workspace_url = (conn.host or '').rstrip('/')
        token = conn.password
        user_account = conn.login
        if not user_account:
            try:
                import requests as _bh_rq
                _bh_me = _bh_rq.get(
                    workspace_url + '/api/2.0/preview/scim/v2/Me',
                    headers={'Authorization': 'Bearer ' + token},
                    timeout=10,
                )
                if _bh_me.status_code == 200:
                    _bh_d = _bh_me.json()
                    user_account = _bh_d.get('userName') or (_bh_d.get('emails') or [{}])[0].get('value')
            except Exception:
                pass
        user_account = user_account or 'unknown'
        if not workspace_url or not token:
            raise ValueError("Databricks connection must have host and password (token)")

        ti.xcom_push(key="bh_audit_metadata", value={
            "databricks_cluster_id": compute_id,
            "databricks_user_account": user_account,
            "ingestion_group_id": None,
            "flow_id": 45
        })

        factory = CloudFactory("databricks", databricks_workspace_url=workspace_url, databricks_token=token)
        compute = factory.get_compute(compute_type="databricks")
        ok = compute.terminate_compute(compute_id, run_async=False)
        logger.info("Terminated cluster %s: %s", compute_id, ok)

    _terminate_params = {}
    delete_compute_cad470ee6 = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='delete_compute_cad470ee6',
        python_callable=terminate_databricks_resources,
        params=_terminate_params,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
        trigger_rule='all_done',
    )


    from airflow.operators.python import PythonOperator
    end_flow_task = PythonOperator(
        task_id='end_flow_task',
        pre_execute=common_task.pre_execute_callback,
        python_callable=common_task.end_dag_task,
        on_success_callback=common_task.flow_success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    start_flow_task >> create_compute_4214f3af1
    create_compute_4214f3af1 >> delete_compute_cad470ee6
    delete_compute_cad470ee6 >> end_flow_task
