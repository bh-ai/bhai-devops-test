
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask

common_task = CommonTask(dag_id='bh_ignite_48', dag_params={})
lineage_task = LineageTask(dag_id='bh_ignite_48', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='bh_ignite_48',
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
            'flow_id': 48,
            'flow_name': 'bh-ignite',
            'flow_key': 'bh_ignite',
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

    spark_app_yaml_bh_ignite_2f64f9e64 = f'''
    apiVersion: "sparkoperator.k8s.io/v1beta2"
    kind: SparkApplication
    metadata:
      name: "spark-app-bh-ignite-2f64f9e64"
      namespace: "spark-operator"
    spec:
      type: Python
      pythonVersion: "3"
      mode: cluster
      image: "bhdwestus3acr01.azurecr.io/bh-transformation-utils-gluten:1.0.0-99-rc"
      imagePullPolicy: Always
      mainApplicationFile: "local:///app/main.py"
      arguments: 
        - "PIPELINE_JSON_B64"
        - "azure"
        - "/app/tmp/schemas/"
      sparkVersion: "3.5.0"
      sparkConf:
        "spark.plugins": "org.apache.gluten.GlutenPlugin"
        "spark.gluten.execution.backend": "velox"
        "spark.shuffle.manager": "org.apache.spark.shuffle.sort.ColumnarShuffleManager"
        "spark.sql.execution.arrow.pyspark.enabled": "true"
        "spark.executorEnv.LD_PRELOAD": "/usr/lib/x86_64-linux-gnu/libjemalloc.so.2"
        "spark.driverEnv.LD_PRELOAD": "/usr/lib/x86_64-linux-gnu/libjemalloc.so.2"
        "spark.driver.extraJavaOptions": "-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true"
        "spark.executor.extraJavaOptions": "-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true"
        "spark.gluten.sql.columnar.backend.velox.glogSeverityLevel": "3"
        "spark.gluten.sql.columnar.backend.velox.glogVerboseLevel": "1"
        "spark.eventLog.compress": "true"
        "spark.hadoop.fs.azure.account.auth.type.bhdevstoacct01lrs.dfs.core.windows.net": "OAuth"
        "spark.hadoop.fs.azure.account.oauth.provider.type.bhdevstoacct01lrs.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider"
        "spark.hadoop.fs.azure.account.oauth2.msi.tenant.bhdevstoacct01lrs.dfs.core.windows.net": "4a548e99-bcbf-4246-86cf-898b4554b7f1"
        "spark.hadoop.fs.azure.account.oauth2.client.id.bhdevstoacct01lrs.dfs.core.windows.net": "f01fd5de-fc42-4ba1-8da8-a6aaff7c5e7e"
        "spark.hadoop.fs.azure.account.hns.enabled.bhdevstoacct01lrs.dfs.core.windows.net": "false"
        "spark.jars.ivy": "/tmp/.ivy2"
        "spark.sql.execution.arrow.maxRecordsPerBatch": "10000"
        "spark.memory.offHeap.enabled": "true"
        "spark.memory.offHeap.size": "2g"
        "spark.eventLog.enabled": "true"
        "spark.eventLog.dir": "abfss://bh-spark-logs@bhdevstoacct01lrs.dfs.core.windows.net/"
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
          workload: "spark-driver"
        tolerations:
          - key: "dedicated"
            operator: "Equal"
            value: "spark-driver"
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
          workload: "spark-executor"
        tolerations:
          - key: "dedicated"
            operator: "Equal"
            value: "spark-executor"
            effect: "NoSchedule"
    '''

    bh_ignite_2f64f9e64 = SparkKubernetesOperator(
        task_id='bh_ignite_2f64f9e64',
        namespace="spark-operator",
        application_file=spark_app_yaml_bh_ignite_2f64f9e64,
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

    start_flow_task >> bh_ignite_2f64f9e64
    bh_ignite_2f64f9e64 >> end_flow_task
