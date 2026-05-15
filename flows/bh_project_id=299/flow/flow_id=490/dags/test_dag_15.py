
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask

common_task = CommonTask(dag_id='test_dag_15', dag_params={})
lineage_task = LineageTask(dag_id='test_dag_15', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='test_dag_15',
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
            'flow_id': 490,
            'flow_name': 'test-dag-15',
            'flow_key': 'test_dag_15',
            'bh_project_id': 299,
            'project_name': 'flow-test-project',
            'flow_tags': [],
            'flow_type': 'INGESTION',
            'tenant_id': 220,
            'flow_status': 'In Progress',
        }
    )


    from airflow.operators.bash import BashOperator
    hello_world1 = BashOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='hello_world1',
        bash_command='echo "Hello world!"',
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )



    from airflow.operators.bash import BashOperator
    hello_world2 = BashOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='hello_world2',
        bash_command='echo "Failing..." && exit 1',
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

    start_flow_task >> hello_world1
    start_flow_task >> hello_world2
    hello_world1 >> end_flow_task
    hello_world2 >> end_flow_task
