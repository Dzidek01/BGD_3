import os
import sys
from pyspark.sql import SparkSession

def main():
    # Define database configuration and paths
    DB_URL = "jdbc:postgresql://db:5432/big_data_postgres"
    DB_PROPERTIES = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }
    FILE_PATH = "data/2019-Nov.csv"
    TARGET_TABLE = "raw_events"

    # Verify file existence before initializing the environment
    if not os.path.exists(FILE_PATH):
        print(f"Error: Target file not found at {FILE_PATH}", file=sys.stderr)
        sys.exit(1)

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Ecommerce_Raw_Ingestion") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    try:
        # Load CSV file
        # inferSchema=True is computationally expensive but necessary for initial loading (Raw layer)
        df = spark.read.csv(FILE_PATH, header=True, inferSchema=True)

        # Write to database
        # mode="overwrite" ensures idempotency - replaces the entire table on subsequent runs
        df.write.jdbc(
            url=DB_URL, 
            table=TARGET_TABLE, 
            mode="overwrite", 
            properties=DB_PROPERTIES
        )

    except Exception as e:
        print(f"Error during Spark execution: {e}", file=sys.stderr)
        sys.exit(1)
        
    finally:
        # Proper session closure releases resources (JVM memory)
        spark.stop()

if __name__ == "__main__":
    main()