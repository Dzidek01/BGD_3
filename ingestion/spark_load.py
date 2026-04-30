import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.functions import from_json, col

def main():
    # Define database configuration and paths
    DB_URL = "jdbc:postgresql://db:5432/big_data_postgres"
    DB_PROPERTIES = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }
    TARGET_TABLE = "raw_events"

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Ecommerce_Raw_Ingestion") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True)
    ])

    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", TARGET_TABLE) \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Cast value to String and deserialize JSON
        parsed_df = df.selectExpr("CAST(value AS STRING) as json_string") \
            .select(from_json(col("json_string"), schema).alias("data")) \
            .select("data.*")

        def write_to_postgres(batch_df, batch_id):
            # This function runs for each micro-batch of data arriving from Kafka
            # Write data to the database in "append" mode
            batch_df.write \
                .jdbc(url=DB_URL, table=TARGET_TABLE, mode="append", properties=DB_PROPERTIES)
            
            count = batch_df.count()
            if count > 0:
                print(f"Saved batch with ID: {batch_id} containing {count} rows.")

        query = parsed_df.writeStream \
            .foreachBatch(write_to_postgres) \
            .outputMode("append") \
            .start()

        query.awaitTermination()

    except Exception as e:
        print(f"Error during Spark execution: {e}", file=sys.stderr)
        sys.exit(1)
        
    finally:
        # Proper session closure releases resources
        spark.stop()

if __name__ == "__main__":
    main()