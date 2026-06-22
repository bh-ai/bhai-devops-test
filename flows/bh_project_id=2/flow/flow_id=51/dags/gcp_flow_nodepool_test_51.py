
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask
import airflow_plugins.dag_task_definitions.feed_control_callbacks as feed_control_callbacks

common_task = CommonTask(dag_id='gcp_flow_nodepool_test_51', dag_params={})
lineage_task = LineageTask(dag_id='gcp_flow_nodepool_test_51', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='gcp_flow_nodepool_test_51',
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
            'flow_id': 51,
            'flow_name': 'gcp-flow-nodepool-test',
            'flow_key': 'gcp_flow_nodepool_test',
            'bh_project_id': 2,
            'project_name': 'flow-service-project',
            'flow_tags': [],
            'flow_type': 'INGESTION',
            'tenant_id': 1,
            'flow_status': 'In Progress',
        }
    )


    from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
    import base64
    import os

    # Read and encode pipeline JSON
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    pipeline_json_path = "/pipelines/bh_project_id=2/pipeline/pipeline_id=6/spark-job-pipeline.json"
    # Strip leading slash to make it relative to the DAG folder
    relative_json_path = pipeline_json_path.lstrip('/')
    full_json_path = os.path.join(dag_folder, relative_json_path)

    try:
        with open(full_json_path, 'r', encoding='utf-8') as f:
            pipeline_json_content = f.read()
        pipeline_json_b64 = base64.b64encode(pipeline_json_content.encode('utf-8')).decode('utf-8')
    except Exception as e:
        print(f"Warning: Could not read pipeline JSON from {full_json_path}: {e}")
        pipeline_json_b64 = ""

    spark_app_yaml_spark_engine_b4f463bdc = f'''
    apiVersion: "sparkoperator.k8s.io/v1beta2"
    kind: SparkApplication
    metadata:
      name: "spark-app-spark-engine-b4f463bdc"
      namespace: "spark-operator"
    spec:
      type: Python
      pythonVersion: "3"
      mode: cluster
      image: "bhdwestus3acr01.azurecr.io/bh-transformation-utils-gluten:1.0.0-80-rc"
      imagePullPolicy: Always
      mainApplicationFile: "local:///app/main.py"
      arguments: 
        - "PIPELINE_JSON_B64"
        - "azure"
        - "/app/tmp/schemas/"
      sparkVersion: "3.5.0"
      sparkConf:
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
        "spark.sql.execution.arrow.pyspark.enabled": "true"
        "spark.sql.adaptive.enabled": "true"
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
        "spark.sql.adaptive.skewJoin.enabled": "true"
        "spark.sql.adaptive.localShuffleReader.enabled": "true"
        "spark.eventLog.compress": "true"
        "spark.jars.ivy": "/tmp/.ivy2"
        "spark.eventLog.enabled": "true"
        "spark.eventLog.dir": "gs://bh-dev-spark-logs/"
      restartPolicy:
        type: Never
      driver:
        cores: 1
        memory: "2g"
        memoryOverhead: "1g"
        labels:
          version: "3.5.0"
          azure.workload.identity/use: "true"
        serviceAccount: spark-operator-spark
        env:
          - name: PIPELINE_JSON_B64
            value: "{pipeline_json_b64}"
        nodeSelector:
          workload: "bh-d-kcl-usea1-spark-driver"
        tolerations:
          - key: "dedicated"
            operator: "Equal"
            value: "bh-d-kcl-usea1-spark-driver"
            effect: "NoSchedule"
      executor:
        cores: 1
        instances: 1
        memory: "2g"
        memoryOverhead: "1g"
        labels:
          version: "3.5.0"
          azure.workload.identity/use: "true"
        env:
          - name: PIPELINE_JSON_B64
            value: "{pipeline_json_b64}"
        nodeSelector:
          workload: "bh-d-kcl-usea1-spark-executor"
        tolerations:
          - key: "dedicated"
            operator: "Equal"
            value: "bh-d-kcl-usea1-spark-executor"
            effect: "NoSchedule"
    '''

    spark_engine_b4f463bdc = SparkKubernetesOperator(
        task_id='spark_engine_b4f463bdc',
        namespace="spark-operator",
        application_file=spark_app_yaml_spark_engine_b4f463bdc,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )



    from airflow.operators.python import PythonOperator
    end_flow_task = PythonOperator(
        task_id='end_flow_task',
        pre_execute=common_task.pre_execute_callback,
        python_callable=common_task.end_dag_task,
        on_success_callback=common_task.flow_success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    start_flow_task >> spark_engine_b4f463bdc
    spark_engine_b4f463bdc >> end_flow_task
