from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime

with DAG(
    "operator_http_example", start_date=datetime(2023, 1, 1), schedule_interval="@daily"
) as dag:
    call_api = SimpleHttpOperator(
        task_id="call_api",
        method="GET",
        http_conn_id="my_http_connection",
        endpoint="api/v1/resource",
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
    )
