import datetime
from airflow import DAG
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator

DAG_NAME = "subdag"

with DAG(
    dag_id=DAG_NAME,
    default_args={"retries": 2},
    start_date=datetime.datetime(2024, 6, 26),
    schedule="@once",
    tags=["subdag"],
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    section_1 = SubDagOperator(
        task_id="section-1",
        subdag=subdag(DAG_NAME, "section-1", dag.default_args),
    )

    some_other_task = EmptyOperator(
        task_id="some-other-task",
    )

    section_2 = SubDagOperator(
        task_id="section-2",
        subdag=subdag(DAG_NAME, "section-2", dag.default_args),
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> section_1 >> some_other_task >> section_2 >> end