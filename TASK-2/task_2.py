from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import json

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "task_2",
    default_args=default_args,
    description="A DAG for predicting gender from names and storing results in PostgreSQL",
    schedule_interval="0 */5 * * *",
    start_date=datetime(2022, 10, 21),
    catchup=False,
)

# Task 1: Fetch predictions from gender-api
fetch_predictions = SimpleHttpOperator(
    task_id="fetch_predictions",
    http_conn_id="gender_api",
    method="POST",
    endpoint="/v2/gender/by-first-name-multiple",
    data='[{"first_name":"Sandra","country":"US"},{"first_name":"Jason","country":"US"},{"first_name":"Miftah", "country": "ID"}]',
    log_response=True,
    dag=dag,
)

# Task 2: Create PostgreSQL table
create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="postgres_connections",
    sql="""
    CREATE TABLE IF NOT EXISTS gender_name_prediction (
        input JSON,
        details JSON,
        result_found BOOLEAN,
        first_name VARCHAR(50),
        probability NUMERIC,
        gender VARCHAR(10),
        timestamp TIMESTAMP WITHOUT TIME ZONE
    );
    """,
    dag=dag,
)


# Task 3: Load predictions into PostgreSQL
def load_predictions_to_postgres(**kwargs):
    ti = kwargs["ti"]
    predictions = ti.xcom_pull(task_ids="fetch_predictions")
    if predictions:
        pg_hook = PostgresHook(postgres_conn_id="postgres_connections")
        insert_query = """
        INSERT INTO gender_name_prediction (input, details, result_found, first_name, probability, gender, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """

        predictions = json.loads(predictions)
        print(predictions[0])

        for prediction in predictions:
            pg_hook.run(
                insert_query,
                parameters=(
                    json.dumps(prediction["input"]),
                    json.dumps(prediction["details"]),
                    prediction["result_found"],
                    prediction["first_name"],
                    prediction["probability"],
                    prediction["gender"],
                    datetime.now(),
                ),
            )


load_predictions = PythonOperator(
    task_id="load_predictions",
    python_callable=load_predictions_to_postgres,
    provide_context=True,
    dag=dag,
)

fetch_predictions >> create_table >> load_predictions
