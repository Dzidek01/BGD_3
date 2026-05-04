FROM apache/airflow:2.9.0-python3.10
RUN pip install --no-cache-dir kafka-python dbt-postgres