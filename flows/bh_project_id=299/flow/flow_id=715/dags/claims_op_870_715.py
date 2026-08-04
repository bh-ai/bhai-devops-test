
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask
import airflow_plugins.dag_task_definitions.feed_control_callbacks as feed_control_callbacks

common_task = CommonTask(dag_id='claims_op_870_715', dag_params={})
lineage_task = LineageTask(dag_id='claims_op_870_715', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='claims_op_870_715',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['dev']
) as dag:


    from airflow.operators.python import PythonOperator
    start_flow_task = PythonOperator(
        task_id='start_flow_task',
        python_callable=common_task.start_dag_task,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
        params = {
            'flow_id': 715,
            'flow_name': 'claims_op_870',
            'flow_key': 'claims_op_870',
            'bh_project_id': 299,
            'project_name': 'flow-test-project',
            'flow_tags': [{'key': 'environment', 'value': 'dev'}],
            'flow_type': 'INGESTION',
            'tenant_id': 220,
            'flow_status': 'In Progress',
        }
    )


    from airflow.operators.python import PythonOperator
    from airflow_plugins.feed_control.sla.inbound.validate_task import (
        validate_inbound_files,
    )
    import airflow_plugins.dag_task_definitions.feed_control_callbacks as feed_control_callbacks

    _validate_params = {
        "bucket_name": "my-test-bucket",
        "source_prefix": "bhargs/3-Claims/3-Claims/Fidelis/Downstate/OP",
        "cloud_type": "databricks",
        "airflow_connection_id": "databricks_default",
        "secret_name": "bh-dev-westus3-kv-key-scope/bh-azureblob-azureblob",
        "filename_regex": None,
        "min_bytes": 1,
        "max_bytes": None,
        "min_files": 1,
        "max_files": None,
        "quarantine_prefix": "bhargs/3-Claims/3-Claims/Fidelis/Downstate/OP/rejected",
        "fail_on_invalid": True,
        "sources": [
            {
                "source_name": "safeharbor_fideliscare_prod_opclaimheader_full_cinqdownstate_20230101_20260331",
                "prefix": "bhargs/3-Claims/3-Claims/Fidelis/Downstate/OP",
                "filename_regex": "safeharbor_FidelisCare_Prod_OPClaimHeader_FULL_CINQDOWNSTATE_20230101_20260331.txt",
                "ignore_subfolders": True,
                "is_required": True,
                "min_bytes": 10,
                "min_files": 1
            },
            {
                "source_name": "safeharbor_fideliscare_prod_opclaimline_full_cinqdownstate_20230101_20260331",
                "prefix": "bhargs/3-Claims/3-Claims/Fidelis/Downstate/OP",
                "filename_regex": "safeharbor_FidelisCare_Prod_OPClaimLine_FULL_CINQDOWNSTATE_20230101_20260331.txt",
                "ignore_subfolders": True,
                "is_required": True,
                "min_bytes": 10,
                "min_files": 1
            }
        ],
        "allow_control_table_failure": False,
        "require_batch_id": None,
        "require_feed_control_policy": True,
        "control_catalog": "cinqdev",
        "control_schema": "control",
        "feed_name": "claims_op",
        "feed_id": "claims_op"
    }
    validate_inbound_files = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='validate_inbound_files',
        python_callable=validate_inbound_files,
        params=_validate_params,
        on_success_callback=feed_control_callbacks.validate_inbound_success_callback,
        on_failure_callback=feed_control_callbacks.validate_inbound_failure_callback,
    )


    from airflow.operators.python import PythonOperator
    from airflow_plugins.compute_pool.databricks_tasks import acquire_databricks_compute_pool

    _acquire_params = {
        "compute_config_id": 168,
        "airflow_connection_id": "databricks_default",
        "ingestion_group_id": 870,
        "flow_id": 715,
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
            "name": "{{ dag.dag_id }}_run_pipelines_claims_op_{{ ts_nodash }}",
            "python_file": "/Workspace/Shared/dev-utils/pipelines/main.py",
            "parameters": [
                "/Workspace/Shared/codespace/pipelines/bh_project_id=299/pipeline/pipeline_id=1504/claims_op.json",
                "databricks",
                "/Workspace/Shared/dev-utils/schemas"
            ]
        },
        "ingestion_group_id": 870,
        "flow_id": 715,
        "pipeline_id": "1504",
        "feed_name": "claims_op",
        "validate_inbound_task_id": "validate_inbound_files",
        "facts_source": "databricks",
        "pipeline_name": "claims_op",
        "airflow_connection_id": "databricks_default",
        "pipeline_key": "claims_op",
        "bh_project_id": 299,
        "project_id": 299,
        "project_name": "flow-test-project",
        "compute_xcom_key": "return_value",
        "valid_files": "{{ task_instance.xcom_pull(task_ids='validate_inbound_files', key='valid_files') }}",
        "batch_id": "{{ task_instance.xcom_pull(task_ids='validate_inbound_files', key='batch_id') }}",
        "batch_control": "{{ ti.xcom_pull(task_ids='validate_inbound_files', key='batch_control') }}",
        "pool_enabled": True,
        "pool_heartbeat_interval_seconds": 120,
        "pool_release_lease": False
    }
    run_pipelines_claims_op = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='run_pipelines_claims_op',
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
            "name": "{{ dag.dag_id }}_run_pipelines_silver_raw_fideliscare_claims_to_pharmacy_pipeline_260804_18b4_{{ ts_nodash }}",
            "python_file": "/Workspace/Shared/dev-utils/pipelines/main.py",
            "parameters": [
                "/Workspace/Shared/codespace/pipelines/bh_project_id=299/pipeline/pipeline_id=1505/silver_raw_fideliscare_claims_to_pharmacy_pipeline_260804_18b4.json",
                "databricks",
                "/Workspace/Shared/dev-utils/schemas"
            ]
        },
        "ingestion_group_id": 870,
        "flow_id": 715,
        "pipeline_id": "1505",
        "feed_name": "claims_op",
        "validate_inbound_task_id": "validate_inbound_files",
        "facts_source": "databricks",
        "pipeline_name": "silver_raw_fideliscare_claims_to_pharmacy_pipeline_260804_18b4",
        "airflow_connection_id": "databricks_default",
        "pipeline_key": "silver_raw_fideliscare_claims_to_pharmacy_pipeline_260804_18b4",
        "bh_project_id": 299,
        "project_id": 299,
        "project_name": "flow-test-project",
        "compute_xcom_key": "return_value",
        "valid_files": "{{ task_instance.xcom_pull(task_ids='validate_inbound_files', key='valid_files') }}",
        "batch_id": "{{ task_instance.xcom_pull(task_ids='validate_inbound_files', key='batch_id') }}",
        "batch_control": "{{ ti.xcom_pull(task_ids='validate_inbound_files', key='batch_control') }}",
        "pool_enabled": True,
        "pool_heartbeat_interval_seconds": 120,
        "pool_release_lease": False
    }
    run_pipelines_silver_raw_fideliscare_claims_to_pharmacy_pipeline_260804_18b4 = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='run_pipelines_silver_raw_fideliscare_claims_to_pharmacy_pipeline_260804_18b4',
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
            "name": "{{ dag.dag_id }}_run_pipelines_claims_silver_ods_fidelis_upstate_ip_{{ ts_nodash }}",
            "python_file": "/Workspace/Shared/dev-utils/pipelines/main.py",
            "parameters": [
                "/Workspace/Shared/codespace/pipelines/bh_project_id=299/pipeline/pipeline_id=1510/claims_silver_ods_fidelis_upstate_ip.json",
                "databricks",
                "/Workspace/Shared/dev-utils/schemas"
            ]
        },
        "ingestion_group_id": 870,
        "flow_id": 715,
        "pipeline_id": "1510",
        "feed_name": "claims_op",
        "validate_inbound_task_id": "validate_inbound_files",
        "facts_source": "databricks",
        "pipeline_name": "claims_silver_ods_fidelis_upstate_IP",
        "airflow_connection_id": "databricks_default",
        "pipeline_key": "claims_silver_ods_fidelis_upstate_ip",
        "bh_project_id": 299,
        "project_id": 299,
        "project_name": "flow-test-project",
        "compute_xcom_key": "return_value",
        "valid_files": "{{ task_instance.xcom_pull(task_ids='validate_inbound_files', key='valid_files') }}",
        "batch_id": "{{ task_instance.xcom_pull(task_ids='validate_inbound_files', key='batch_id') }}",
        "batch_control": "{{ ti.xcom_pull(task_ids='validate_inbound_files', key='batch_control') }}",
        "pool_enabled": True,
        "pool_heartbeat_interval_seconds": 120,
        "pool_release_lease": True
    }
    run_pipelines_claims_silver_ods_fidelis_upstate_ip = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='run_pipelines_claims_silver_ods_fidelis_upstate_ip',
        python_callable=submit_job_to_cluster,
        params=_submit_params,
        on_success_callback=feed_control_callbacks.submit_job_success_callback,
        on_failure_callback=feed_control_callbacks.submit_job_failure_callback,
    )


    from airflow.operators.python import PythonOperator
    import os
    from airflow.hooks.base import BaseHook
    from airflow_plugins.cloud_factory import CloudFactory

    def archive_processed_files(**context):
        params = context.get("params") or {}
        bucket_name = params.get("bucket_name")
        archive_prefix = params.get("archive_prefix")
        cloud_type = (params.get("cloud_type") or "aws").lower()
        if not bucket_name:
            raise ValueError("bucket_name is required for ArchiveProcessedFiles")
        if not archive_prefix:
            raise ValueError("archive_prefix is required for ArchiveProcessedFiles")
        if cloud_type not in ["aws", "gcp", "azure", "databricks"]:
            raise ValueError(f"Unsupported cloud_type '{cloud_type}' for ArchiveProcessedFiles")

        input_files = params.get("files")
        if isinstance(input_files, str) and "{" in input_files:
            input_files = context["task"].render_template(input_files, context)
            if isinstance(input_files, str):
                import ast
                try:
                    input_files = ast.literal_eval(input_files)
                except (ValueError, SyntaxError):
                    logger.warning("Rendered input_files is not valid Python literal", extra={"rendered_input_files": input_files})
                    input_files = None
        if input_files is None:
            input_files = []
        elif not isinstance(input_files, list):
            input_files = [input_files] if input_files else []
        source_prefix = (params.get("source_prefix") or "").lstrip("/")
        airflow_connection_id = params.get("airflow_connection_id", "aws_default")
        delete_source = params.get("delete_source", True)
        allow_empty = params.get("allow_empty", True)

        conn = BaseHook.get_connection(airflow_connection_id)
        extras = conn.extra_dejson or {}
        factory_kwargs = {}
        if cloud_type == "aws":
            if conn.login:
                factory_kwargs["aws_access_key_id"] = conn.login
            if conn.password:
                factory_kwargs["aws_secret_access_key"] = conn.password
            factory_kwargs["region"] = (
                extras.get("region_name")
                or extras.get("region")
                or extras.get("aws_region")
                or "us-east-1"
            )
        elif cloud_type == "gcp":
            if extras.get("project"):
                factory_kwargs["project_id"] = extras.get("project")
            elif extras.get("project_id"):
                factory_kwargs["project_id"] = extras.get("project_id")
            if extras.get("keyfile_dict"):
                factory_kwargs["credentials"] = extras.get("keyfile_dict")
            elif extras.get("key_path"):
                factory_kwargs["credentials_path"] = extras.get("key_path")
        elif cloud_type == "azure":
            factory_kwargs["connection_string"] = (
                conn.password
                or extras.get("connection_string")
                or extras.get("azure_storage_connection_string")
            )
            if extras.get("account_url"):
                factory_kwargs["account_url"] = extras.get("account_url")
        elif cloud_type == "databricks":
            workspace_url = (conn.host or "").rstrip("/")
            token = conn.password
            if not workspace_url or not token:
                raise ValueError(
                    "Databricks connection must have host and password (token) for ArchiveProcessedFiles"
                )
            factory_kwargs["databricks_workspace_url"] = workspace_url
            factory_kwargs["databricks_token"] = token
            # Keep archive task aligned with compute pattern: pass Databricks
            # secrets context to cloud-factory, let provider resolve backend creds.
            storage_backend = extras.get("storage_backend")
            if storage_backend:
                factory_kwargs["storage_backend"] = storage_backend
            account_url = extras.get("account_url")
            if account_url:
                factory_kwargs["account_url"] = account_url

            storage_secret_name = (
                params.get("secret_name")
                or extras.get("secret_name")
            )
            if storage_secret_name:
                factory_kwargs["storage_secret_name"] = storage_secret_name
                # Enforce Azure Blob path when secret-based storage is requested.
                factory_kwargs["storage_backend"] = "azure_blob"

            secrets_scope_name = extras.get("secrets_scope_name")
            if secrets_scope_name:
                factory_kwargs["secrets_scope_name"] = secrets_scope_name

            secrets_backend_type = extras.get("secrets_backend_type")
            if not secrets_backend_type:
                secrets_backend_type = "databricks"
            if secrets_backend_type:
                factory_kwargs["secrets_backend_type"] = secrets_backend_type

        factory = CloudFactory(cloud_type, **factory_kwargs)
        storage = factory.get_storage(cloud_type)
        ti = context["ti"]
        ds_nodash = context.get("ds_nodash") or "unknown_date"

        def _is_truthy_folder_flag(value):
            return str(value).strip().lower() in {"true", "1", "yes"}

        def _is_directory_like_object(obj):
            key = str((obj or {}).get("key") or "")
            if not key or key.endswith("/"):
                return True
            metadata = (obj or {}).get("metadata") or {}
            if isinstance(metadata, dict) and _is_truthy_folder_flag(metadata.get("hdi_isfolder")):
                return True
            return False

        def _extract_source_key(item):
            if isinstance(item, str):
                return item
            if isinstance(item, dict):
                return (
                    item.get("key")
                    or item.get("source_key")
                    or item.get("object_key")
                    or item.get("path")
                )
            return None

        files_to_archive = []
        if isinstance(input_files, list):
            for item in input_files:
                source_key = _extract_source_key(item)
                if source_key:
                    files_to_archive.append(source_key)

        if not files_to_archive:
            objects_with_metadata = []
            if hasattr(storage, "list_objects_with_metadata"):
                objects_with_metadata = storage.list_objects_with_metadata(
                    bucket_name=bucket_name,
                    prefix=source_prefix,
                ) or []

            if objects_with_metadata:
                files_to_archive = [
                    str(obj.get("key"))
                    for obj in objects_with_metadata
                    if not _is_directory_like_object(obj)
                ]
            else:
                files_to_archive = storage.list_objects(bucket_name=bucket_name, prefix=source_prefix) or []
                files_to_archive = [k for k in files_to_archive if k and not k.endswith("/")]

        if not files_to_archive and not allow_empty:
            raise ValueError("No files found to archive")

        archived_files = []
        for source_key in files_to_archive:
            base_name = os.path.basename(source_key)
            dest_key = f"{archive_prefix.strip('/')}/{ds_nodash}/{base_name}"
            if delete_source:
                moved = storage.move_object(
                    source_bucket_name=bucket_name,
                    source_object_key=source_key,
                    destination_bucket_name=bucket_name,
                    destination_object_key=dest_key,
                )
                if not moved:
                    raise RuntimeError(
                        f"Failed to move '{source_key}' to '{dest_key}' for cloud_type '{cloud_type}'"
                    )
            else:
                copied = storage.copy_object(
                    source_bucket_name=bucket_name,
                    source_object_key=source_key,
                    destination_bucket_name=bucket_name,
                    destination_object_key=dest_key,
                )
                if not copied:
                    raise RuntimeError(
                        f"Failed to copy '{source_key}' to '{dest_key}' for cloud_type '{cloud_type}'"
                    )
            archived_files.append({"source_key": source_key, "archive_key": dest_key})

        summary = {
            "cloud_type": cloud_type,
            "bucket_name": bucket_name,
            "archive_prefix": archive_prefix,
            "archived_count": len(archived_files),
            "delete_source": delete_source,
            "archived_files": archived_files,
        }
        ti.xcom_push(key="archived_files", value=archived_files)
        ti.xcom_push(key="archive_summary", value=summary)
        return summary

    _archive_params = {
        "bucket_name": "my-test-bucket",
        "source_prefix": "",
        "archive_prefix": "Archive/bhargs/3-Claims/3-Claims/Fidelis/Downstate/OP",
        "cloud_type": "databricks",
        "airflow_connection_id": "databricks_default",
        "secret_name": "bh-dev-westus3-kv-key-scope/bh-azureblob-azureblob",
        "delete_source": True,
        "allow_empty": True,
        "files": "{{ task_instance.xcom_pull(task_ids='validate_inbound_files', key='valid_files') }}"
    }
    archive_processed_files = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='archive_processed_files',
        python_callable=archive_processed_files,
        params=_archive_params,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )


    from airflow.operators.python import PythonOperator
    from airflow_plugins.compute_pool.databricks_tasks import safety_release_compute_pool

    _release_params = {
        "compute_task_id": "create_compute",
        "ingestion_group_id": 870,
        "flow_id": 715
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

    start_flow_task >> validate_inbound_files
    validate_inbound_files >> create_compute
    create_compute >> run_pipelines_claims_op
    run_pipelines_claims_op >> run_pipelines_silver_raw_fideliscare_claims_to_pharmacy_pipeline_260804_18b4
    create_compute >> run_pipelines_silver_raw_fideliscare_claims_to_pharmacy_pipeline_260804_18b4
    run_pipelines_silver_raw_fideliscare_claims_to_pharmacy_pipeline_260804_18b4 >> run_pipelines_claims_silver_ods_fidelis_upstate_ip
    create_compute >> run_pipelines_claims_silver_ods_fidelis_upstate_ip
    run_pipelines_claims_silver_ods_fidelis_upstate_ip >> archive_processed_files
    archive_processed_files >> delete_compute
    create_compute >> delete_compute
    delete_compute >> end_flow_task
