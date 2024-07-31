from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime


def choose_branch():
    return "branch_a" if some_condition else "branch_b"


with DAG(
    "operator_branch_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
) as dag:
    branch_task = BranchPythonOperator(
        task_id="branch_task", python_callable=choose_branch
    )
    branch_a = DummyOperator(task_id="branch_a")
    branch_b = DummyOperator(task_id="branch_b")

    branch_task >> [branch_a, branch_b]
