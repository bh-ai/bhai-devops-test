
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask
import airflow_plugins.dag_task_definitions.feed_control_callbacks as feed_control_callbacks

common_task = CommonTask(dag_id='bh_oracle_jar_bh_env_nojar_1_1_v50_65', dag_params={})
lineage_task = LineageTask(dag_id='bh_oracle_jar_bh_env_nojar_1_1_v50_65', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='bh_oracle_jar_bh_env_nojar_1_1_v50_65',
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
            'flow_id': 65,
            'flow_name': 'bh_oracle_jar_bh_env_nojar_1_1_v50',
            'flow_key': 'bh_oracle_jar_bh_env_nojar_1_1_v50',
            'bh_project_id': 2,
            'project_name': 'flow-service-project',
            'flow_tags': [],
            'flow_type': 'INGESTION',
            'tenant_id': 1,
            'flow_status': 'In Progress',
        }
    )


    from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator    
    create_compute = EmrCreateJobFlowOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='create_compute',
        aws_conn_id='None',
        emr_conn_id=None,
        wait_for_completion=True,
        job_flow_overrides={"Name": "emr-cluster01", "ReleaseLabel": "emr-7.6.0", "LogUri": null, "Instances": {"InstanceGroups": [{"Name": "Primary node", "Market": "ON_DEMAND", "InstanceRole": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1}, {"Name": "Core nodes", "Market": "ON_DEMAND", "InstanceRole": "CORE", "InstanceType": "m5.xlarge", "InstanceCount": 1}], "Ec2SubnetId": null, "EmrManagedMasterSecurityGroup": null, "EmrManagedSlaveSecurityGroup": null, "ServiceAccessSecurityGroup": null, "KeepJobFlowAliveWhenNoSteps": True, "TerminationProtected": False, "Ec2KeyName": null}, "Applications": [{"Name": "Spark"}], "AutoTerminationPolicy": {"IdleTimeout": 1800}, "Tags": [], "VisibleToAllUsers": True, "JobFlowRole": "EMR_EC2_DefaultRole", "ServiceRole": "EMR_DefaultRole", "Steps": [{"Name": "Cluster Setup", "ActionOnFailure": "TERMINATE_CLUSTER", "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["bash", "-c", "\nset -euxo pipefail;\n\necho \"Setup started\";\n\n# Download necessary files from S3\naws s3 cp \"s3:///scripts/main.py\" /tmp/pipeline.py\naws s3 cp \"s3:////transformation-utils/bh_transformation_utils--py3-none-any.whl\" /tmp/bh_transformation_utils--py3-none-any.whl\naws s3 cp \"s3:///pipeline/\" /tmp/schemas/pipeline/ --recursive\naws s3 cp \"s3:///jars/\" /tmp/jars/ --recursive\n\n# Validate downloaded files\n[ -f /tmp/pipeline.py ] || (echo \"pipeline.py not found\" && exit 1)\n[ -f /tmp/bh_transformation_utils--py3-none-any.whl ] || (echo \"bh_transformation_utils--py3-none-any.whl not found\" && exit 1)\n[ -d /tmp/schemas ] || (echo \"schemas directory not found\" && exit 1)\n[ -d /tmp/jars ] || (echo \"jars directory not found\" && exit 1)\n\n# Install dependencies\n/usr/bin/python3.11 -m pip install /tmp/bh_transformation_utils--py3-none-any.whl[aws]\n"]}}]},
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )



    from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator    
    submit_bh_oracle_jar_bh_env_nojar_1_1_v50 = EmrAddStepsOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='submit_bh_oracle_jar_bh_env_nojar_1_1_v50',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_compute', key='return_value') }}",
        aws_conn_id="None",
        wait_for_completion=True,
        steps=[{"Name": "Pipeline Job (bh_oracle_jar_bh_env_nojar_1_1_v50)", "ActionOnFailure": "TERMINATE_JOB_FLOW", "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["bash", "-c", "\nset -euxo pipefail;\n\nexport PYSPARK_PYTHON=/usr/bin/python3.11\nexport PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11\n\necho \"Starting PySpark Job: bh_oracle_jar_bh_env_nojar_1_1_v50\"\nspark-submit --deploy-mode cluster --jars s3://bh-utils/jars/postgresql-42.7.12.jar --files s3://bh-utils/bh-ui-aws-project/pipelines/bh_project_id=3/pipeline/pyspark/pipeline_id=41/bh_postgres_join_kafka/connections.yml --py-files s3://bh-utils/boto3_deps.zip s3://pyspark/bh_oracle_jar_bh_env_nojar_1_1_v50/main.py\necho \"PySpark Job Completed\"\n"]}}],
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )



    from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator    
    delete_compute = EmrTerminateJobFlowOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='delete_compute',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_compute', key='return_value') }}",
        aws_conn_id='None',
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

    start_flow_task >> create_compute
    create_compute >> submit_bh_oracle_jar_bh_env_nojar_1_1_v50
    submit_bh_oracle_jar_bh_env_nojar_1_1_v50 >> delete_compute
    create_compute >> delete_compute
    delete_compute >> end_flow_task
