from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime

with DAG(
    "operator_s3_to_redshift_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
) as dag:
    copy_data = S3ToRedshiftOperator(
        task_id="copy_data",
        schema="public",
        table="my_table",
        s3_bucket="my_bucket",
        s3_key="data/my_data.csv",
        copy_options=["csv"],
        aws_conn_id="my_aws_connection",
        redshift_conn_id="my_redshift_connection",
    )
