from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

import logging
import requests


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['mdownload812@gmail.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 6, 25),
}

dag = DAG(
    'covid19_india_etl',
    default_args = default_args,
    description = 'An ETL Pipeline for Covid 19 India Data',
    schedule_interval = timedelta(days=1),
    tags = ['real_life_example'],   
)

def fetch_covid19_india_data(url):
    response = requests.get(url)
    if response.status_code != 200:
        logging.error(f"Error while fetchin data: {response.status_code}")
    logging.info(f'Data has been fetched successfully. {response.status_code}')
    return response.json()

def extract_data(**kwargs):
    url = 'https://data.covid19india.org/v4/min/timeseries.min.json'
    data = fetch_covid19_india_data(url)
    kwargs['ti'].xcom_push(key='raw_data', value=data)

def transform_data(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(key='raw_data')
    transformed_data = []
    for state, state_data in raw_data.items():
        for date, metrics in state_data['dates'].items():
            total_metrics = metrics.get('total', {})
            entry = {
                'date': date,
                'state': state,
                'confirmed': total_metrics.get('confirned', 0),
                'recovered': total_metrics.get('recovered', 0),
                'deceased': total_metrics.get('deceased', 0),
            }
            transformed_data.append(entry)
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)

def load_data(**kwargs):
    transfromed_data = kwargs['ti'].xcom_pull(key='transformed_data')
    hook = PostgresHook(postgres_conn_id='covid19_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()

    for entry in transfromed_data:
        cursor.execute("""
            INSERT INTO covid19_india_data (date, state, confirmed, recovered, deceased)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (date, state) DO UPDATE
            SET confirmed = EXCLUDED.confirmed,
                recovered = EXCLUDED.recovered,
                deceased = EXCLUDED.deceased;
        """, (entry['date'], entry['state'], entry['confirmed'], entry['recovered'], entry['deceased']))

    conn.commit()
    cursor.close()
    conn.close()


extract_task = PythonOperator(
    task_id = 'extract_data',
    python_callable = extract_data,
    provide_context = True,
    dag = dag
)

transform_task = PythonOperator(
    task_id = 'transform_data',
    python_callable = transform_data,
    provide_context = True,
    dag = dag
)

load_task = PythonOperator(
    task_id = 'load_data',
    python_callable = load_data,
    provide_context = True,
    dag = dag
)

extract_task >> transform_task >> load_task