from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {"owner": "coder2j", "retries": 5, "retry_delay": timedelta(minutes=2)}


with DAG(
    dag_id="my_first_dag_v4",
    default_args=default_args,
    description="This is our first dag that we write",
    start_date=datetime(2024, 7, 29, 2),
    schedule_interval="@daily",
) as dag:
    task1 = BashOperator(
        task_id="first_task", bash_command="echo hello world, ini adalah task1!"
    )

    task2 = BashOperator(
        task_id="second_task",
        bash_command="echo hey, Saya task2 dan akan jalan bersama task1!",
    )

    task3 = BashOperator(
        task_id="thrid_task",
        bash_command="echo hey, Saya task3 dan akan jalan setelah task1 dan task2!",
    )

    # Task dependency method 1
    [task1, task2] >> task3
