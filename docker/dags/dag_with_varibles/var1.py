from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def get_var_func():
    nama = Variable.get("nama")
    usia = Variable.get("usia")

    print(f"Halo Nama saya {nama}, saya berusia {usia}")
    # print(f"Print variables, book_entities  {book_entities_var}")


def get_var_context_func(**context):
    return "Print variables: "


dag = DAG(
    "var_v1",
    description="var_v1",
    schedule_interval="@daily",
    start_date=datetime(2024, 7, 21, 12),
    catchup=False,
)

get_var = PythonOperator(
    task_id="get_var", provide_context=True, python_callable=get_var_func, dag=dag
)


# get_var_context = PythonOperator(
#     task_id="get_var_context", python_callable=get_var_context_func, dag=dag
# )

get_var
# >> get_var_context
