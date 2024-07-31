from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def print_hello():
    print("Hello, World!")


with DAG(
    dag_id="operator_python_example",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@monthly",
) as dag:
    run_python = PythonOperator(task_id="run_python", python_callable=print_hello)
