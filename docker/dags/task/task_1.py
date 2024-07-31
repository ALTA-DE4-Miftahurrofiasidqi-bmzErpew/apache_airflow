from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

desc = "Create DAG that run in every 5 hours. \
Suppose we define a new task that push a variable to xcom.\
How to pull multiple values at once?"

# Create DAG that run in every 5 hours
dag = DAG(
    dag_id="task_1",
    default_args=default_args,
    description=desc,
    schedule_interval="0 */5 * * *",
    start_date=days_ago(1),
    catchup=False,
)


def push_variables(**kwargs):
    ti = kwargs["ti"]
    person = {"first_name": "Miftahurrofi", "last_name": "Asid-Qi", "age": 23}
    ti.xcom_push(key="person", value=person)


def pull_variables(**kwargs):
    ti = kwargs["ti"]
    person = ti.xcom_pull(key="person", task_ids="push_task")

    first_name = person["first_name"]
    last_name = person["last_name"]
    age = person["age"]

    print("=================================================")
    print(f"Hello my name is {first_name} {last_name}, I am {age} years old.")
    print("=================================================")


# Task to push variables to XCom
push_task = PythonOperator(
    task_id="push_task",
    python_callable=push_variables,
    provide_context=True,
    dag=dag,
)

# Task to pull multiple values at once from XCom
pull_task = PythonOperator(
    task_id="pull_task",
    python_callable=pull_variables,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
push_task >> pull_task
