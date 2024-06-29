from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('consumer_dag', default_args=default_args, schedule_interval='@daily', catchup=False, tags=["external_task_sensor"]) as dag:

    wait_for_producer = ExternalTaskSensor(
        task_id = 'wait_for_producer',
        external_dag_id = 'producer_dag',
        external_task_id = 'produce',
        allowed_states = ['success'],
        execution_delta = timedelta(hours=-1), # ensures the sensor waits for the same day execution date
        check_existence = True
    )

    start = BashOperator (
        task_id ='start',
        bash_command ='echo "Starting..."'
    )

    consume = BashOperator (
        task_id ='produce',
        bash_command ='echo "Consuming..."'
    )

    end = BashOperator (
        task_id ='end',
        bash_command ='echo "Ending..."'
    )

    wait_for_producer >> start >> consume >> end