
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask

common_task = CommonTask(dag_id='dbricks_03', dag_params={})
lineage_task = LineageTask(dag_id='dbricks_03', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='dbricks_03',
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
            'flow_id': 15,
            'flow_name': 'dbricks-03',
            'flow_key': 'dbricks_03',
            'bh_project_id': 2,
            'project_name': 'flow-service-project',
            'bh_environment_id': 1,
            'bh_environment_name': 'azure-westus3',
            'bh_kc_secret_url': None,
            'flow_tags': [],
            'flow_type': 'INGESTION',
            'tenant_id': 1,
            'cloud_provider': 'azure',
            'vault_url': 'https://bh-dev-westus3-kv-key.vault.azure.net',
            'flow_status': 'In Progress',
        }
    )

    from airflow.operators.python import PythonOperator
    from airflow.providers.databricks.hooks.databricks import DatabricksHook

    def create_databricks_cluster_custom_0704938fa(**context):
        from airflow_plugins.cloud_factory import CloudFactory
        hook = DatabricksHook(databricks_conn_id='databricks_deafult')
        conn = hook.get_conn()
        workspace_url = (conn.host or '').rstrip('/')
        token = conn.password
        if not workspace_url or not token:
            raise ValueError("Databricks connection must have host and password (token)")
        factory = CloudFactory("databricks", databricks_workspace_url=workspace_url, databricks_token=token)
        compute = factory.get_compute(compute_type="databricks")
        payload = (
            {
                "created_at": "2026-04-28T05:19:31.103192Z",
                "updated_at": "2026-04-28T05:19:31.103192Z",
                "created_by": "sathish@bighammer.ai",
                "updated_by": "sathish@bighammer.ai",
                "is_deleted": False,
                "deleted_by": None,
                "tenant_id": 1,
                "compute_config_id": 1,
                "compute_config_name": "test-01",
                "cloud_provider": "databricks",
                "compute_type": "DATABRICKS_CLUSTER",
                "bh_env_id": 2,
                "bh_env_name": "dbricks-westus",
                "compute_config": {
                    "cluster_name": "flow_service_databricks_AK_Local_0303_V1",
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
                    "spark_env_vars": {},
                    "custom_tags": {},
                    "cluster_log_conf": {
                        "test": "sathish"
                    },
                    "init_scripts": [
                        "/Workspace/Shared/bh-dev-utils/scripts/bh_databricks_grpc_server.sh"
                    ],
                    "libraries": [],
                    "databricks_region": None,
                    "bh_tags": []
                },
                "compute_profile": None,
                "cluster_name": "dbricks_test_cluster",
                "spark_env_vars": {
                    "SECRET_MANAGER_PROVIDER": "databricks"
                }
            }
        )
        cluster_id = compute.create_compute(
            payload,
            compute_name=payload.get("cluster_name"),
            run_async=False,
        )
        if not cluster_id:
            raise ValueError("create_compute did not return cluster_id")
        return cluster_id

    custom_0704938fa = PythonOperator(
        task_id='custom_0704938fa',
        python_callable=create_databricks_cluster_custom_0704938fa,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )


    from airflow.operators.python import PythonOperator
    end_flow_task = PythonOperator(
        task_id='end_flow_task',
        pre_execute=common_task.pre_execute_callback,
        python_callable=common_task.end_dag_task,
        on_failure_callback=common_task.failure_callback
    )

    start_flow_task >> custom_0704938fa
    custom_0704938fa >> end_flow_task
