from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'sample_etl',
    default_args=default_args,
    description='ETL DAG for products data from dummyjson API',
    schedule_interval=timedelta(days=1),
)

def extract():
    url = 'https://dummyjson.com/products'
    response = requests.get(url)
    data = response.json() 
    return data['products']

def transform(**context):
    data = context['task_instance'].xcom_pull(task_ids='extract_task')
    transformed_data = [item for item in data if item['price'] <= 100]
    return transformed_data

def load(**context):
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'sample_etl',
    default_args=default_args,
    description='ETL DAG for products data from dummyjson API',
    schedule_interval=timedelta(days=1),
)

def extract():
    url = 'https://dummyjson.com/products'
    response = requests.get(url)
    data = response.json() 
    return data['products']

def transform(**context):
    data = context['task_instance'].xcom_pull(task_ids='extract_task')
    transformed_data = [item for item in data if item['price'] <= 100]
    return transformed_data

def load(**context):
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'sample_etl',
    default_args=default_args,
    description='ETL DAG for products data from dummyjson API',
    schedule_interval=timedelta(days=1),
)

def extract():
    url = 'https://dummyjson.com/products'
    response = requests.get(url)
    data = response.json() 
    return data['products']

def transform(**context):
    data = context['task_instance'].xcom_pull(task_ids='extract_task')
    transformed_data = [item for item in data if item['price'] <= 100]
    return transformed_data

def load(**context):
    transformed_data = context['task_instance'].xcom_pull(task_ids='transform_task')
    print("Transformed Data: ", json.dumps(transformed_data, indent=2))

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task