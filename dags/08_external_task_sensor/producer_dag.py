from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 25)
}

with DAG('producer_dag', default_args=default_args, schedule_interval='@daily', catchup=False, tags=["external_task_sensor"]) as dag:

    start = BashOperator (
        task_id ='start',
        bash_command ='echo "Starting..."'
    )

    produce = BashOperator (
        task_id ='produce',
        bash_command ='echo "Producing..."'
    )

    end = BashOperator (
        task_id ='end',
        bash_command ='echo "Ending..."'
    )

    start >> produce >> end