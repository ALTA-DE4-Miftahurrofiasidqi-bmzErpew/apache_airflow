from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime

with DAG(
    "operator_mysql_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
) as dag:
    run_sql = MySqlOperator(
        task_id="run_sql",
        mysql_conn_id="my_mysql_connection",
        sql="""CREATE TABLE IF NOT EXISTS my_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50));""",
    )
