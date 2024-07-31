from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

with DAG(
    dag_id="operator_dummy_example",
    start_date=datetime(2024, 7, 20),
    schedule_interval="@daily",
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    start >> end
