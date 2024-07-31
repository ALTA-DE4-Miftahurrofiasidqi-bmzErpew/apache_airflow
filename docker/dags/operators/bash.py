from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

with DAG(
    dag_id="operator_bash_example",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
) as dag:
    run_bash = BashOperator(task_id="run_bash", bash_command='echo "Hello, World!"')

run_bash
