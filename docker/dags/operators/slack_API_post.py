from airflow import DAG
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from datetime import datetime

with DAG(
    "operator_slack_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
) as dag:
    send_slack_message = SlackAPIPostOperator(
        task_id="send_slack_message",
        token="xoxb-your-slack-token",
        username="airflow",
        text="Hello from Airflow!",
        channel="#general",
    )
