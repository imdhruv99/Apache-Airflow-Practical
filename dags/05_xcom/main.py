from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Task to push data to XCom
def push_data(**kwargs):
    ti = kwargs['ti']
    data = "Hello from task1!"
    ti.xcom_push(key='my_key', value=data)

# Task to pull data from XCom
def pull_data(**kwargs):
    ti = kwargs['ti']
    pulled_data = ti.xcom_pull(key='my_key', task_ids='push_task')
    print(f"Pulled data: {pulled_data}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 22),
}

with DAG(dag_id='xcom_dag', default_args=default_args, schedule_interval='@daily', catchup=False, tags=["xcom_pull", "xcom_push"]) as dag:

    push_task = PythonOperator(
        task_id='push_task',
        python_callable=push_data,
        provide_context=True
    )

    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_data,
        provide_context=True
    )

    push_task >> pull_task
