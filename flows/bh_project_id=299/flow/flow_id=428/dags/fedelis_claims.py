
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask

common_task = CommonTask(dag_id='fedelis_claims', dag_params={})
lineage_task = LineageTask(dag_id='fedelis_claims', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='fedelis_claims',
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
            'flow_id': 428,
            'flow_name': 'fedelis_claims',
            'flow_key': 'fedelis_claims',
            'bh_project_id': 299,
            'project_name': 'flow-test-project',
            'flow_tags': [],
            'flow_type': 'INGESTION',
            'tenant_id': 220,
            'flow_status': 'In Progress',
        }
    )

    from airflow.operators.python import PythonOperator
    from airflow.providers.databricks.hooks.databricks import DatabricksHook

    def create_databricks_cluster_create_compute_0df4a4d68(**context):
        from airflow_plugins.cloud_factory import CloudFactory
        hook = DatabricksHook(databricks_conn_id='databricks_default')
        conn = hook.get_conn()
        workspace_url = (conn.host or '').rstrip('/')
        token = conn.password
        if not workspace_url or not token:
            raise ValueError("Databricks connection must have host and password (token)")
        factory = CloudFactory("databricks", databricks_workspace_url=workspace_url, databricks_token=token)
        compute = factory.get_compute(compute_type="databricks")
        payload = (
            {
                "cluster_name": "fedelis_claims",
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
                    "/Workspace/Shared/bh-dev-utils/scripts/bh_test_databricks_grpc_server_codeartifact.sh"
                ],
                "libraries": [],
                "databricks_region": "us-west-1",
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
        return cluster_id

    create_compute_0df4a4d68 = PythonOperator(
        task_id='create_compute_0df4a4d68',
        python_callable=create_databricks_cluster_create_compute_0df4a4d68,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    from airflow.operators.python import PythonOperator
    from airflow_plugins.cloud_factory import CloudFactory
    import logging
    logger = logging.getLogger(__name__)

    def submit_job_to_cluster(**context):
        params = context.get("params") or {}
        job_config = params.get("job_config")
        if not job_config:
            raise ValueError("Missing job_config in params")

        # Prefer compute_id from params (supports Jinja xcom_pull strings), fallback to XCom.
        compute_id = params.get("compute_id")
        xcom_key = str(params.get("compute_xcom_key") or "return_value")
        if not compute_id or (isinstance(compute_id, str) and "{" in compute_id):
            ti = context["ti"]
            # Most flows normalize the create task_id to 'create_compute'. Keep a legacy fallback.
            compute_task_id = params.get("compute_task_id") or "create_compute"
            compute_id = ti.xcom_pull(task_ids=compute_task_id, key=xcom_key)
            if not compute_id:
                compute_id = ti.xcom_pull(task_ids="databricks_create_cluster_task", key=xcom_key)

        if not compute_id or (isinstance(compute_id, str) and "{" in compute_id):
            raise ValueError("No compute_id from params or XCom")

        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('databricks_default')
        workspace_url = (conn.host or '').rstrip('/')
        token = conn.password
        if not workspace_url or not token:
            raise ValueError("Databricks connection must have host and password (token)")

        factory = CloudFactory("databricks", databricks_workspace_url=workspace_url, databricks_token=token)
        compute = factory.get_compute(compute_type="databricks")
        result = compute.execute_job(compute_id, job_config, run_async=False)
        if result.get("status") == "FAILED":
            raise RuntimeError(result.get("error", "Job submission failed"))
        run_id = result.get("run_id")
        if run_id:
            context["ti"].xcom_push(key="run_id", value=run_id)
        return result

    _submit_params = {
        "compute_task_id": "create_compute_0df4a4d68",
        "job_config": {
            "job_type": "spark_python",
            "name": "{{ dag.dag_id }}_submit_job_6e14c8a43_{{ ts_nodash }}",
            "python_file": "/Workspace/Shared/bh-dev-utils/pipelines/main.py",
            "parameters": [
                "/Workspace/Shared/codespace/test/pipelines/bh_project_id=299/pipeline/pipeline_id=659/fidelis_cinqdownstate_1_248.json",
                "databricks",
                "/Workspace/Shared/bh-dev-utils/schemas"
            ]
        },
        "compute_xcom_key": "return_value"
    }
    submit_job_6e14c8a43 = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='submit_job_6e14c8a43',
        python_callable=submit_job_to_cluster,
        params=_submit_params,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    from airflow.operators.python import PythonOperator
    from airflow_plugins.cloud_factory import CloudFactory
    import logging
    logger = logging.getLogger(__name__)

    def submit_job_to_cluster(**context):
        params = context.get("params") or {}
        job_config = params.get("job_config")
        if not job_config:
            raise ValueError("Missing job_config in params")

        # Prefer compute_id from params (supports Jinja xcom_pull strings), fallback to XCom.
        compute_id = params.get("compute_id")
        xcom_key = str(params.get("compute_xcom_key") or "return_value")
        if not compute_id or (isinstance(compute_id, str) and "{" in compute_id):
            ti = context["ti"]
            # Most flows normalize the create task_id to 'create_compute'. Keep a legacy fallback.
            compute_task_id = params.get("compute_task_id") or "create_compute"
            compute_id = ti.xcom_pull(task_ids=compute_task_id, key=xcom_key)
            if not compute_id:
                compute_id = ti.xcom_pull(task_ids="databricks_create_cluster_task", key=xcom_key)

        if not compute_id or (isinstance(compute_id, str) and "{" in compute_id):
            raise ValueError("No compute_id from params or XCom")

        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('databricks_default')
        workspace_url = (conn.host or '').rstrip('/')
        token = conn.password
        if not workspace_url or not token:
            raise ValueError("Databricks connection must have host and password (token)")

        factory = CloudFactory("databricks", databricks_workspace_url=workspace_url, databricks_token=token)
        compute = factory.get_compute(compute_type="databricks")
        result = compute.execute_job(compute_id, job_config, run_async=False)
        if result.get("status") == "FAILED":
            raise RuntimeError(result.get("error", "Job submission failed"))
        run_id = result.get("run_id")
        if run_id:
            context["ti"].xcom_push(key="run_id", value=run_id)
        return result

    _submit_params = {
        "compute_task_id": "create_compute_0df4a4d68",
        "job_config": {
            "job_type": "spark_python",
            "name": "{{ dag.dag_id }}_submit_job_10147fb46_{{ ts_nodash }}",
            "python_file": "/Workspace/Shared/bh-dev-utils/pipelines/main.py",
            "parameters": [
                "/Workspace/Shared/codespace/test/pipelines/bh_project_id=299/pipeline/pipeline_id=660/fidelis_cinqdownstate_2_248.json",
                "databricks",
                "/Workspace/Shared/bh-dev-utils/schemas"
            ]
        },
        "compute_xcom_key": "return_value"
    }
    submit_job_10147fb46 = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='submit_job_10147fb46',
        python_callable=submit_job_to_cluster,
        params=_submit_params,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    from airflow.operators.python import PythonOperator
    from airflow_plugins.cloud_factory import CloudFactory
    import logging
    logger = logging.getLogger(__name__)

    def submit_job_to_cluster(**context):
        params = context.get("params") or {}
        job_config = params.get("job_config")
        if not job_config:
            raise ValueError("Missing job_config in params")

        # Prefer compute_id from params (supports Jinja xcom_pull strings), fallback to XCom.
        compute_id = params.get("compute_id")
        xcom_key = str(params.get("compute_xcom_key") or "return_value")
        if not compute_id or (isinstance(compute_id, str) and "{" in compute_id):
            ti = context["ti"]
            # Most flows normalize the create task_id to 'create_compute'. Keep a legacy fallback.
            compute_task_id = params.get("compute_task_id") or "create_compute"
            compute_id = ti.xcom_pull(task_ids=compute_task_id, key=xcom_key)
            if not compute_id:
                compute_id = ti.xcom_pull(task_ids="databricks_create_cluster_task", key=xcom_key)

        if not compute_id or (isinstance(compute_id, str) and "{" in compute_id):
            raise ValueError("No compute_id from params or XCom")

        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('databricks_default')
        workspace_url = (conn.host or '').rstrip('/')
        token = conn.password
        if not workspace_url or not token:
            raise ValueError("Databricks connection must have host and password (token)")

        factory = CloudFactory("databricks", databricks_workspace_url=workspace_url, databricks_token=token)
        compute = factory.get_compute(compute_type="databricks")
        result = compute.execute_job(compute_id, job_config, run_async=False)
        if result.get("status") == "FAILED":
            raise RuntimeError(result.get("error", "Job submission failed"))
        run_id = result.get("run_id")
        if run_id:
            context["ti"].xcom_push(key="run_id", value=run_id)
        return result

    _submit_params = {
        "compute_task_id": "create_compute_0df4a4d68",
        "job_config": {
            "job_type": "spark_python",
            "name": "{{ dag.dag_id }}_submit_job_fee4eaf0a_{{ ts_nodash }}",
            "python_file": "/Workspace/Shared/bh-dev-utils/pipelines/main.py",
            "parameters": [
                "/Workspace/Shared/codespace/test/pipelines/bh_project_id=299/pipeline/pipeline_id=661/silver_raw_fidelis_claims_to_cinqcare_silver_260510_7434.json",
                "databricks",
                "/Workspace/Shared/bh-dev-utils/schemas"
            ]
        },
        "compute_xcom_key": "return_value"
    }
    submit_job_fee4eaf0a = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='submit_job_fee4eaf0a',
        python_callable=submit_job_to_cluster,
        params=_submit_params,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    from airflow.operators.python import PythonOperator
    from airflow_plugins.cloud_factory import CloudFactory
    import logging
    logger = logging.getLogger(__name__)

    def terminate_databricks_resources(**context):
        ti = context["ti"]
        compute_id = ti.xcom_pull(task_ids="create_compute_0df4a4d68", key="return_value")
        if not compute_id or (isinstance(compute_id, str) and "{" in compute_id):
            params = context.get("params") or {}
            compute_id = params.get("compute_id")
        if not compute_id or (isinstance(compute_id, str) and "{" in compute_id):
            logger.warning("No compute_id from XCom task create_compute_0df4a4d68 or params; skipping terminate")
            return
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('databricks_default')
        workspace_url = (conn.host or '').rstrip('/')
        token = conn.password
        if not workspace_url or not token:
            raise ValueError("Databricks connection must have host and password (token)")
        factory = CloudFactory("databricks", databricks_workspace_url=workspace_url, databricks_token=token)
        compute = factory.get_compute(compute_type="databricks")
        ok = compute.terminate_compute(compute_id, run_async=False)
        logger.info("Terminated cluster %s: %s", compute_id, ok)

    _terminate_params = {}
    delete_compute_a10478f2d = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='delete_compute_a10478f2d',
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
        on_failure_callback=common_task.failure_callback
    )

    start_flow_task >> create_compute_0df4a4d68
    create_compute_0df4a4d68 >> submit_job_6e14c8a43
    submit_job_6e14c8a43 >> submit_job_10147fb46
    create_compute_0df4a4d68 >> submit_job_10147fb46
    submit_job_10147fb46 >> submit_job_fee4eaf0a
    create_compute_0df4a4d68 >> submit_job_fee4eaf0a
    submit_job_fee4eaf0a >> delete_compute_a10478f2d
    create_compute_0df4a4d68 >> delete_compute_a10478f2d
    delete_compute_a10478f2d >> end_flow_task
