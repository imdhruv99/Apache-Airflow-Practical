from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hello_world',
    default_args=default_args,
    description='A simple Hello World ETL pipeline',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    print_hello_world = BashOperator(
    task_id='print_hello',
    bash_command='echo "hello world"'
    )

    print_name = BashOperator(
    task_id='print_name',
    bash_command='echo "DHRUV PRAJAPATI"'
    )

    print_hello_world >> print_name