from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'amogh',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='stock_pipeline',
    default_args=default_args,
    description='NSE stock data pipeline: Kafka -> Spark -> Snowflake -> dbt',
    schedule_interval='30 9 * * 1-5',  # 9:30 AM IST on weekdays (market open)
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['stocks', 'kafka', 'snowflake', 'dbt'],
) as dag:

    check_kafka = BashOperator(
        task_id='check_kafka',
        bash_command='docker exec kafka kafka-topics --list --bootstrap-server localhost:9092',
    )

    start_producer = BashOperator(
        task_id='start_producer',
        bash_command='docker restart stock-producer',
    )

    start_spark = BashOperator(
        task_id='start_spark_streaming',
        bash_command='docker restart spark-streaming',
    )

    wait_for_data = BashOperator(
        task_id='wait_for_data',
        bash_command='sleep 120',  # wait 2 mins for data to land in Snowflake
    )

    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt_project/stock_pipeline && /home/airflow/.local/bin/dbt run --profiles-dir /home/airflow/.dbt',
    )

    test_dbt = BashOperator(
        task_id='test_dbt_models',
        bash_command='cd /opt/airflow/dbt_project/stock_pipeline && /home/airflow/.local/bin/dbt test --profiles-dir /home/airflow/.dbt',
    )

    check_kafka >> start_producer >> start_spark >> wait_for_data >> run_dbt >> test_dbt
