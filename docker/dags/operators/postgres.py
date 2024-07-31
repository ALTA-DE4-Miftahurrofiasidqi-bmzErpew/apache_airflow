from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

with DAG(
    "operator_postgres_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
) as dag:
    run_sql = PostgresOperator(
        task_id="run_sql",
        postgres_conn_id="my_postgres_connection",
        sql="""CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY, name VARCHAR(50));""",
    )
