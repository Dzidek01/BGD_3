from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='ecommerce_big_data_pipeline',
    default_args=default_args,
    description='Automatyczny potok ETL: Kafka -> Spark -> Postgres -> dbt',
    schedule=timedelta(hours=1), 
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ecommerce', 'big_data'],
) as dag:

    # Start producer
    run_producer = BashOperator(
        task_id='run_kafka_producer',
        bash_command='python /opt/airflow/project/ingestion/kafka_producer.py'
    )

    # Start dbt
    run_dbt = BashOperator(
        task_id='run_dbt_transformations',
        bash_command='cd /opt/airflow/project/dbt_ecommerce && dbt clean && dbt run --profiles-dir .'
    )

    run_producer >> run_dbt