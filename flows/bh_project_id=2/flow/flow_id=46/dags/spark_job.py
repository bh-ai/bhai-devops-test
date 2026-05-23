
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask

common_task = CommonTask(dag_id='spark_job', dag_params={})
lineage_task = LineageTask(dag_id='spark_job', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='spark_job',
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
            'flow_id': 46,
            'flow_name': 'spark-job',
            'flow_key': 'spark_job',
            'bh_project_id': 2,
            'project_name': 'flow-service-project',
            'flow_tags': [],
            'flow_type': 'INGESTION',
            'tenant_id': 1,
            'flow_status': 'In Progress',
        }
    )


    from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

    spark_app_yaml_spark_operator_b5a4ce9ad = '''
    apiVersion: "sparkoperator.k8s.io/v1beta2"
    kind: SparkApplication
    metadata:
      name: "spark-app-{{ run_id | lower | regex_replace('[^a-z0-9\\-]', '-') }}-spark_operator_b5a4ce9ad"
      namespace: "spark-jobs"
    spec:
      type: Python
      pythonVersion: "3"
      mode: cluster
      image: "transformation-utils:latest"
      imagePullPolicy: Always
      mainApplicationFile: "local:///app/main.py"
      arguments: 
        - "pipeline-design-muthu-test.json"
        - "azure"
        - "/app/schemas/"
      sparkVersion: "3.4.0"
      sparkConf:
        "spark.eventLog.enabled": "true"
        "spark.eventLog.dir": "abfss://spark-logs@YOUR_STORAGE_ACCOUNT.dfs.core.windows.net/"
      restartPolicy:
        type: Never
      driver:
        cores: 1
        memory: "2g"
        labels:
          version: 3.4.0
        serviceAccount: spark-operator-spark
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
        labels:
          version: 3.4.0
        nodeSelector:
          workload: "spark-executor"
        tolerations:
          - key: "dedicated"
            operator: "Equal"
            value: "spark-executor"
            effect: "NoSchedule"
    '''

    spark_operator_b5a4ce9ad = SparkKubernetesOperator(
        task_id='spark_operator_b5a4ce9ad',
        namespace="spark-jobs",
        application_file=spark_app_yaml_spark_operator_b5a4ce9ad,
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

    start_flow_task >> spark_operator_b5a4ce9ad
    spark_operator_b5a4ce9ad >> end_flow_task
