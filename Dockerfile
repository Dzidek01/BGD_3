FROM apache/airflow:2.9.0-python3.10

USER root
RUN apt-get update && \
    apt-get install -y docker.io && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir kafka-python dbt-postgres