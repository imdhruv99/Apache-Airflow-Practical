from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

def fetch_data_from_postgres():
    hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    sql = "SELECT * FROM test_table;"
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    print(f"Executing query: {sql}")
    print(f"Fetched results: {result}")
    for row in result:
        print(row)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 24),
}

with DAG(
    dag_id='connection_and_hook',
    default_args=default_args,
    schedule_interval='@daily', 
    catchup=False,
    tags=["connection_and_hook"],
) as dag:
    
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable = fetch_data_from_postgres
    )

    fetch_data