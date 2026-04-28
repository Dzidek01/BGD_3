import subprocess
from prefect import flow, task

@task(name="Extract & Load (Spark)")
def run_spark():
    # Execute the PySpark ingestion script inside the Docker container (Bronze Layer)
    cmd = "docker exec spark_worker /opt/spark/bin/spark-submit --packages org.postgresql:postgresql:42.6.0 ingestion/spark_load.py"
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    
    # Fail the task and raise an exception if the command execution fails
    if result.returncode != 0:
        raise Exception(f"Spark data ingestion failed:\n{result.stderr}")

@task(name="Transform (dbt)")
def run_dbt():
    # Navigate to the dbt project directory and execute the models (Silver & Gold Layers)
    cmd = "cd dbt_ecommerce && dbt run --profiles-dir ."
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    
    # Fail the task and raise an exception if the dbt transformation fails
    if result.returncode != 0:
        raise Exception(f"dbt transformation failed:\n{result.stdout}\n{result.stderr}")

@flow(name="E-commerce Data Pipeline")
def main_flow():
    # Execute the pipeline sequentially: extraction followed by transformation
    run_spark()
    run_dbt()

if __name__ == "__main__":
    # Script entry point
    main_flow()