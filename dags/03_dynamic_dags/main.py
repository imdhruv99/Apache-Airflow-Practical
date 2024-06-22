from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_hello_world():
    print("Hello World..!")
    
def print_name():
    print("DHRUV PRAJAPATI")

def create_dag(dag_id, schedule_interval, start_date):
    dag = DAG(dag_id, schedule_interval=schedule_interval, start_date=start_date, catchup=False, tags=["dynamic_dags"],)

    with dag:
        task1 = PythonOperator(
            task_id='task1',
            python_callable=print_hello_world
        )
        
        task2 = PythonOperator(
            task_id='task2',
            python_callable=print_name
        )

        task1 >> task2

    return dag

dag_ids = ['dynamic_dag_1', 'dynamic_dag_2', 'dynamic_dag_3']

for dag_id in dag_ids:
    globals()[dag_id] = create_dag(dag_id, '@hourly', datetime(2024, 6, 22))
