
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask
import airflow_plugins.dag_task_definitions.feed_control_callbacks as feed_control_callbacks

common_task = CommonTask(dag_id='claim_op_868_714', dag_params={})
lineage_task = LineageTask(dag_id='claim_op_868_714', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='claim_op_868_714',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['12346', 'dev']
) as dag:


    from airflow.operators.python import PythonOperator
    start_flow_task = PythonOperator(
        task_id='start_flow_task',
        python_callable=common_task.start_dag_task,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
        params = {
            'flow_id': 714,
            'flow_name': 'claim_op_868',
            'flow_key': 'claim_op_868',
            'bh_project_id': 104,
            'project_name': 'bighammer',
            'flow_tags': [{'key': 'drn', 'value': '12346'}, {'key': 'environment', 'value': 'dev'}],
            'flow_type': 'INGESTION',
            'tenant_id': 113,
            'flow_status': 'In Progress',
        }
    )

    from airflow.operators.python import PythonOperator
    from airflow_plugins.compute_pool.databricks_tasks import acquire_databricks_compute_pool

    _acquire_params = {
        "compute_config_id": 159,
        "airflow_connection_id": "databricks_default",
        "ingestion_group_id": 868,
        "flow_id": 714,
        "pool_namespace": "default"
    }
    create_compute = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='create_compute',
        python_callable=acquire_databricks_compute_pool,
        params=_acquire_params,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    from airflow.operators.python import PythonOperator
    from airflow_plugins.compute_pool.databricks_tasks import submit_job_to_cluster

    _submit_params = {
        "compute_task_id": "create_compute",
        "job_config": {
            "job_type": "spark_python",
            "name": "{{ dag.dag_id }}_run_jobs_claim_op_{{ ts_nodash }}",
            "python_file": "/Workspace/Shared/dev-utils/pipelines/main.py",
            "parameters": [
                "/Workspace/Shared/codespace/pipelines/bh_project_id=104/pipeline/pipeline_id=1488/claim_op.json",
                "databricks",
                "/Workspace/Shared/dev-utils/schemas"
            ]
        },
        "ingestion_group_id": 868,
        "flow_id": 714,
        "pipeline_id": "1488",
        "pipeline_name": "claim_op",
        "airflow_connection_id": "databricks_default",
        "pipeline_key": "claim_op",
        "bh_project_id": 104,
        "project_id": 104,
        "project_name": "bighammer",
        "compute_xcom_key": "return_value",
        "pool_enabled": True,
        "pool_heartbeat_interval_seconds": 120,
        "pool_release_lease": False
    }
    run_jobs_claim_op = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='run_jobs_claim_op',
        python_callable=submit_job_to_cluster,
        params=_submit_params,
        on_success_callback=feed_control_callbacks.submit_job_success_callback,
        on_failure_callback=feed_control_callbacks.submit_job_failure_callback,
    )

    from airflow.operators.python import PythonOperator
    from airflow_plugins.compute_pool.databricks_tasks import submit_job_to_cluster

    _submit_params = {
        "compute_task_id": "create_compute",
        "job_config": {
            "job_type": "spark_python",
            "name": "{{ dag.dag_id }}_run_jobs_silver_raw_fideliscare_op_claims_to_pharmacy_etl_260803_fd46_{{ ts_nodash }}",
            "python_file": "/Workspace/Shared/dev-utils/pipelines/main.py",
            "parameters": [
                "/Workspace/Shared/codespace/pipelines/bh_project_id=104/pipeline/pipeline_id=1489/silver_raw_fideliscare_op_claims_to_pharmacy_etl_260803_fd46.json",
                "databricks",
                "/Workspace/Shared/dev-utils/schemas"
            ]
        },
        "ingestion_group_id": 868,
        "flow_id": 714,
        "pipeline_id": "1489",
        "pipeline_name": "silver_raw_fideliscare_op_claims_to_pharmacy_etl_260803_fd46",
        "airflow_connection_id": "databricks_default",
        "pipeline_key": "silver_raw_fideliscare_op_claims_to_pharmacy_etl_260803_fd46",
        "bh_project_id": 104,
        "project_id": 104,
        "project_name": "bighammer",
        "compute_xcom_key": "return_value",
        "pool_enabled": True,
        "pool_heartbeat_interval_seconds": 120,
        "pool_release_lease": False
    }
    run_jobs_silver_raw_fideliscare_op_claims_to_pharmacy_etl_260803_fd46 = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='run_jobs_silver_raw_fideliscare_op_claims_to_pharmacy_etl_260803_fd46',
        python_callable=submit_job_to_cluster,
        params=_submit_params,
        on_success_callback=feed_control_callbacks.submit_job_success_callback,
        on_failure_callback=feed_control_callbacks.submit_job_failure_callback,
    )

    from airflow.operators.python import PythonOperator
    from airflow_plugins.compute_pool.databricks_tasks import submit_job_to_cluster

    _submit_params = {
        "compute_task_id": "create_compute",
        "job_config": {
            "job_type": "spark_python",
            "name": "{{ dag.dag_id }}_run_jobs_cinqcare_member_file_8_21_25_582_{{ ts_nodash }}",
            "python_file": "/Workspace/Shared/dev-utils/pipelines/main.py",
            "parameters": [
                "/Workspace/Shared/codespace/pipelines/bh_project_id=104/pipeline/pipeline_id=1069/cinqcare_member_file_8_21_25_582.json",
                "databricks",
                "/Workspace/Shared/dev-utils/schemas"
            ]
        },
        "ingestion_group_id": 868,
        "flow_id": 714,
        "pipeline_id": "1069",
        "pipeline_name": "CinqCare Member File 8_21_25_582",
        "airflow_connection_id": "databricks_default",
        "pipeline_key": "cinqcare_member_file_8_21_25_582",
        "bh_project_id": 104,
        "project_id": 104,
        "project_name": "bighammer",
        "compute_xcom_key": "return_value",
        "pool_enabled": True,
        "pool_heartbeat_interval_seconds": 120,
        "pool_release_lease": True
    }
    run_jobs_cinqcare_member_file_8_21_25_582 = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='run_jobs_cinqcare_member_file_8_21_25_582',
        python_callable=submit_job_to_cluster,
        params=_submit_params,
        on_success_callback=feed_control_callbacks.submit_job_success_callback,
        on_failure_callback=feed_control_callbacks.submit_job_failure_callback,
    )

    from airflow.operators.python import PythonOperator
    from airflow_plugins.compute_pool.databricks_tasks import safety_release_compute_pool

    _release_params = {
        "compute_task_id": "create_compute",
        "ingestion_group_id": 868,
        "flow_id": 714
    }
    delete_compute = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='delete_compute',
        python_callable=safety_release_compute_pool,
        params=_release_params,
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

    start_flow_task >> create_compute
    create_compute >> run_jobs_claim_op
    run_jobs_claim_op >> run_jobs_silver_raw_fideliscare_op_claims_to_pharmacy_etl_260803_fd46
    create_compute >> run_jobs_silver_raw_fideliscare_op_claims_to_pharmacy_etl_260803_fd46
    run_jobs_silver_raw_fideliscare_op_claims_to_pharmacy_etl_260803_fd46 >> run_jobs_cinqcare_member_file_8_21_25_582
    create_compute >> run_jobs_cinqcare_member_file_8_21_25_582
    run_jobs_cinqcare_member_file_8_21_25_582 >> delete_compute
    create_compute >> delete_compute
    delete_compute >> end_flow_task
