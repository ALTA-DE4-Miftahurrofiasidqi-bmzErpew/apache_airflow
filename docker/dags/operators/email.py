from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from datetime import datetime

with DAG(
    dag_id="operator_email_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
) as dag:
    send_email = EmailOperator(
        task_id="send_email",
        to="example@example.com",
        subject="Airflow Email Test",
        html_content="<p>This is a test email from Airflow</p>",
    )
