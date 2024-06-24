from airflow import DAG
from datetime import datetime
from custom_operator import HttpGetOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 24)
}

with DAG(
    dag_id='custom_operator',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=["custom_operator"],
) as dag:
    
    get_data = HttpGetOperator(
        task_id='get_data',
        endpoint='https://dummyjson.com/products'
    )

    get_data