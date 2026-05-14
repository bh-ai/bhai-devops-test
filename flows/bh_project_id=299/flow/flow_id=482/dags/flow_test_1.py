
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask

common_task = CommonTask(dag_id='flow_test_1', dag_params={})
lineage_task = LineageTask(dag_id='flow_test_1', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='flow_test_1',
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
            'flow_id': 482,
            'flow_name': 'flow_test_1',
            'flow_key': 'flow_test_1',
            'bh_project_id': 299,
            'project_name': 'flow-test-project',
            'flow_tags': [],
            'flow_type': 'INGESTION',
            'tenant_id': 220,
            'flow_status': 'In Progress',
        }
    )


    from airflow.operators.python import PythonOperator
    import os
    import re
    from airflow.hooks.base import BaseHook
    from airflow_plugins.cloud_factory import CloudFactory

    def validate_inbound_files(**context):
        params = context.get("params") or {}
        bucket_name = params.get("bucket_name")
        if not bucket_name:
            raise ValueError("bucket_name is required for ValidateInboundFiles")

        source_prefix = (params.get("source_prefix") or "").lstrip("/")
        cloud_type = (params.get("cloud_type") or "aws").lower()
        if cloud_type not in ["aws", "gcp", "azure", "databricks"]:
            raise ValueError(f"Unsupported cloud_type '{cloud_type}' for ValidateInboundFiles")
        airflow_connection_id = params.get("airflow_connection_id", "aws_default")
        quarantine_prefix = params.get("quarantine_prefix")
        fail_on_invalid = params.get("fail_on_invalid", True)
        secret_name = params.get("secret_name")
        configured_sources = params.get("sources") or []
        ds_nodash = context.get("ds_nodash") or "unknown_date"

        if configured_sources and not isinstance(configured_sources, list):
            raise ValueError("sources must be an array when provided")

        # Backward compatibility path: build single-source definition from top-level fields.
        if not configured_sources:
            configured_sources = [{
                "source_name": "default",
                "prefix": source_prefix,
                "filename_regex": params.get("filename_regex"),
                "ignore_subfolders": False,
                "is_required": True,
                "required_patterns": [],
                "min_bytes": params.get("min_bytes", 1),
                "max_bytes": params.get("max_bytes"),
                "min_files": params.get("min_files", 1),
                "max_files": params.get("max_files"),
            }]

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
                    "Databricks connection must have host and password (token) for ValidateInboundFiles"
                )
            factory_kwargs["databricks_workspace_url"] = workspace_url
            factory_kwargs["databricks_token"] = token
            storage_secret_name = secret_name or extras.get("secret_name")
            if storage_secret_name:
                factory_kwargs["storage_secret_name"] = storage_secret_name
                factory_kwargs["storage_backend"] = "azure_blob"
            else:
                raise ValueError(
                    "secret_name is required for databricks validation storage. "
                    "Use value format 'scope/key' to avoid separate scope config."
                )
            if extras.get("secrets_scope_name"):
                factory_kwargs["secrets_scope_name"] = extras.get("secrets_scope_name")
            factory_kwargs["secrets_backend_type"] = extras.get("secrets_backend_type") or "databricks"

        factory = CloudFactory(cloud_type, **factory_kwargs)
        storage = factory.get_storage(cloud_type)
        valid_files = []
        invalid_files = []
        missing_required = []
        valid_keys_in_any_source = set()

        def _validate_numeric_rules(min_bytes, max_bytes, min_files, max_files):
            if min_bytes is not None and min_bytes < 0:
                raise ValueError("min_bytes must be >= 0")
            if max_bytes is not None and max_bytes < 0:
                raise ValueError("max_bytes must be >= 0")
            if min_bytes is not None and max_bytes is not None and min_bytes > max_bytes:
                raise ValueError("min_bytes cannot be greater than max_bytes")
            if min_files is not None and min_files < 0:
                raise ValueError("min_files must be >= 0")
            if max_files is not None and max_files < 1:
                raise ValueError("max_files must be >= 1")
            if min_files is not None and max_files is not None and min_files > max_files:
                raise ValueError("min_files cannot be greater than max_files")

        for source in configured_sources:
            source_name = (source or {}).get("source_name") or "default"
            source_rule_prefix = ((source or {}).get("prefix") or source_prefix).lstrip("/")
            filename_regex = (source or {}).get("filename_regex") or params.get("filename_regex")
            ignore_subfolders = bool((source or {}).get("ignore_subfolders", False))
            is_required = (source or {}).get("is_required", True)
            required_patterns = (source or {}).get("required_patterns") or []
            # Backward compatibility: if old required_patterns are present, preserve behavior.
            if required_patterns:
                required_regexes = [re.compile(pattern) for pattern in required_patterns]
            else:
                required_regexes = [re.compile(filename_regex)] if (is_required and filename_regex) else []
            min_bytes = (source or {}).get("min_bytes")
            if min_bytes is None:
                min_bytes = params.get("min_bytes", 1)
            max_bytes = (source or {}).get("max_bytes")
            if max_bytes is None:
                max_bytes = params.get("max_bytes")
            min_files = (source or {}).get("min_files")
            if min_files is None:
                min_files = params.get("min_files", 1)
            # Source optionality takes precedence over file-count minimum.
            if not is_required:
                min_files = 0
            max_files = (source or {}).get("max_files")
            if max_files is None:
                max_files = params.get("max_files")
            _validate_numeric_rules(min_bytes, max_bytes, min_files, max_files)

            regex = re.compile(filename_regex) if filename_regex else None

            objects_with_metadata = []
            if hasattr(storage, "list_objects_with_metadata"):
                objects_with_metadata = storage.list_objects_with_metadata(
                    bucket_name=bucket_name,
                    prefix=source_rule_prefix,
                ) or []
            if objects_with_metadata:
                candidate_files = [
                    {
                        "key": obj.get("key"),
                        "size_bytes": int(obj.get("size", obj.get("size_bytes", 0)) or 0),
                    }
                    for obj in objects_with_metadata
                    if obj.get("key") and not str(obj.get("key")).endswith("/")
                ]
            else:
                keys = storage.list_objects(bucket_name=bucket_name, prefix=source_rule_prefix) or []
                candidate_files = [
                    {"key": key, "size_bytes": 0}
                    for key in keys
                    if key and not key.endswith("/")
                ]

            valid_in_source = []
            invalid_in_source = []
            required_hits = [False for _ in required_regexes]
            for item in candidate_files:
                key = item["key"]
                base_name = os.path.basename(key)
                relative_key = key.lstrip("/")
                normalized_source_prefix = source_rule_prefix.strip("/")
                if normalized_source_prefix:
                    source_prefix_with_slash = f"{normalized_source_prefix}/"
                    if relative_key.startswith(source_prefix_with_slash):
                        relative_key = relative_key[len(source_prefix_with_slash):]
                reasons = []
                file_size = int(item.get("size_bytes", 0))
                regex_target = relative_key
                if ignore_subfolders and "/" in relative_key:
                    continue

                if regex and not regex.match(regex_target):
                    reasons.append(
                        f"filename '{regex_target}' does not match regex "
                        f"(ignore_subfolders={ignore_subfolders})"
                    )

                if min_bytes is not None and file_size < min_bytes:
                    reasons.append(f"file size {file_size} is below min_bytes {min_bytes}")
                if max_bytes is not None and file_size > max_bytes:
                    reasons.append(f"file size {file_size} exceeds max_bytes {max_bytes}")

                file_info = {
                    "source_name": source_name,
                    "key": key,
                    "size_bytes": file_size,
                    "base_name": base_name,
                    "relative_key": relative_key,
                }
                if reasons:
                    file_info["reasons"] = reasons
                    invalid_in_source.append(file_info)
                else:
                    valid_keys_in_any_source.add(key)
                    valid_in_source.append(file_info)
                    for idx, req_re in enumerate(required_regexes):
                        if req_re.match(regex_target):
                            required_hits[idx] = True

            valid_count = len(valid_in_source)
            total_count = len(candidate_files)
            if min_files is not None and valid_count < min_files:
                invalid_in_source.append({
                    "source_name": source_name,
                    "key": "__batch_rule__",
                    "size_bytes": 0,
                    "base_name": "__batch_rule__",
                    "reasons": [f"valid file count {valid_count} below min_files {min_files}"],
                })
            if max_files is not None and valid_count > max_files:
                invalid_in_source.append({
                    "source_name": source_name,
                    "key": "__batch_rule__",
                    "size_bytes": 0,
                    "base_name": "__batch_rule__",
                    "reasons": [f"valid file count {valid_count} above max_files {max_files}"],
                })

            required_pattern_labels = required_patterns or ([filename_regex] if (is_required and filename_regex) else [])
            for idx, pattern in enumerate(required_pattern_labels):
                if not required_hits[idx]:
                    rule_item = {
                        "source_name": source_name,
                        "required_pattern": pattern,
                        "reason": "required pattern not found",
                    }
                    missing_required.append(rule_item)
                    invalid_in_source.append({
                        "source_name": source_name,
                        "key": "__required_rule__",
                        "size_bytes": 0,
                        "base_name": "__required_rule__",
                        "reasons": [f"required pattern '{pattern}' not found"],
                    })

            valid_files.extend(valid_in_source)
            invalid_files.extend(invalid_in_source)

        quarantined_keys = []
        already_quarantined = set()
        filtered_invalid_files = []
        for item in invalid_files:
            key = item.get("key")
            if key in ("__batch_rule__", "__required_rule__") or key not in valid_keys_in_any_source:
                filtered_invalid_files.append(item)
        if quarantine_prefix and filtered_invalid_files:
            normalized_quarantine = quarantine_prefix.strip("/")
            for item in filtered_invalid_files:
                invalid_key = item.get("key")
                if not invalid_key or invalid_key in ("__batch_rule__", "__required_rule__"):
                    continue
                if invalid_key in valid_keys_in_any_source:
                    continue
                if invalid_key in already_quarantined:
                    continue
                target_key = f"{normalized_quarantine}/{ds_nodash}/{os.path.basename(invalid_key)}"
                moved = storage.move_object(
                    source_bucket_name=bucket_name,
                    source_object_key=invalid_key,
                    destination_bucket_name=bucket_name,
                    destination_object_key=target_key,
                )
                if not moved:
                    raise RuntimeError(f"Failed to quarantine '{invalid_key}' to '{target_key}'")
                already_quarantined.add(invalid_key)
                quarantined_keys.append(target_key)

        summary = {
            "cloud_type": cloud_type,
            "bucket_name": bucket_name,
            "source_prefix": source_prefix,
            "total_files": len(valid_files) + len([i for i in filtered_invalid_files if i.get("key") not in ("__batch_rule__", "__required_rule__")]),
            "valid_files": valid_files,
            "invalid_files": filtered_invalid_files,
            "missing_required": missing_required,
            "quarantined_keys": quarantined_keys,
        }
        context["ti"].xcom_push(key="valid_files", value=valid_files)
        context["ti"].xcom_push(key="invalid_files", value=filtered_invalid_files)
        context["ti"].xcom_push(key="missing_required", value=missing_required)
        context["ti"].xcom_push(key="validation_summary", value=summary)

        if fail_on_invalid and filtered_invalid_files:
            raise ValueError(
                f"ValidateInboundFiles found {len(filtered_invalid_files)} invalid items"
            )

        return summary

    _validate_params = {
        "bucket_name": "my-test-bucket",
        "source_prefix": "claims-cms-ndjson/test/",
        "cloud_type": "databricks",
        "airflow_connection_id": "databricks_default",
        "secret_name": "bh-dev-westus3-kv-key-scope/bh-azureblob-landingazureakv1",
        "filename_regex": None,
        "min_bytes": 1,
        "max_bytes": None,
        "min_files": 1,
        "max_files": None,
        "quarantine_prefix": None,
        "fail_on_invalid": True,
        "sources": [
            {
                "source_name": "test",
                "prefix": "tets",
                "filename_regex": "",
                "ignore_subfolders": True,
                "is_required": True,
                "required_patterns": [],
                "min_bytes": 1,
                "max_bytes": 1000,
                "min_files": 1,
                "max_files": 10
            }
        ]
    }
    validate_inbound_files_9ff46d21d = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='validate_inbound_files_9ff46d21d',
        python_callable=validate_inbound_files,
        params=_validate_params,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )



    from airflow.operators.python import PythonOperator
    end_flow_task = PythonOperator(
        task_id='end_flow_task',
        pre_execute=common_task.pre_execute_callback,
        python_callable=common_task.end_dag_task,
        on_success_callback=common_task.flow_success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    start_flow_task >> validate_inbound_files_9ff46d21d
    validate_inbound_files_9ff46d21d >> end_flow_task
