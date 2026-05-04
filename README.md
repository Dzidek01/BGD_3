# E-Commerce Big Data Pipeline (Streaming & Batch)

An end-to-end, fully containerized Data Engineering pipeline demonstrating modern streaming and batch processing capabilities. The project ingests raw e-commerce events, streams them through Apache Kafka, processes them with Apache Spark, and builds a Medallion Architecture data warehouse in PostgreSQL using dbt. The entire workflow is orchestrated via Apache Airflow.

## Data Layers (Medallion Architecture)

### 1. Bronze (Raw Layer)

- **Table:** `raw_events`
- **Process:** A Python producer reads the massive raw `.csv` file and pushes events to a Kafka topic. Apache Spark (Structured Streaming) continuously consumes this topic and writes micro-batches directly to PostgreSQL via JDBC. Data is kept in its original, raw state.

### 2. Silver (Cleansed Layer)

- **Tables:** `silver_products`, `silver_events`
- **Process:** dbt cleans and models the data. It deduplicates product catalogs (keeping only the latest product states) and filters out invalid, incomplete, or corrupted user events to provide a reliable foundation for analytics.

### 3. Gold (Analytics Layer)

- **Table:** `gold`
- **Process:** dbt aggregates the cleaned data to create business-critical reporting. This layer includes comprehensive brand metrics (e.g., total views, conversion rates, revenues) enabling direct business intelligence insights.

## Tech Stack

This project utilizes a modern, open-source data stack designed for scalability, stream processing, and full automation.

- **Infrastructure:** [Docker](https://www.docker.com/) & [Docker Compose](https://docs.docker.com/compose/) with custom image builds.
- **Message Broker:** [Apache Kafka](https://kafka.apache.org/) for real-time event streaming.
- **Data Processing:** [Apache Spark (PySpark)](https://spark.apache.org/docs/latest/api/python/index.html) for scalable, fault-tolerant structured streaming.
- **Data Warehouse:** [PostgreSQL](https://www.postgresql.org/) serving as the centralized relational storage.
- **Transformation:** [dbt (data build tool)](https://www.getdbt.com/) for SQL-based modeling and ELT processes.
- **Orchestration:** [Apache Airflow](https://airflow.apache.org/) for workflow automation, task dependencies, and pipeline monitoring.

## Architecture

```mermaid
graph TD
    %% Orchestration
    subgraph Orchestration ["Orchestration"]
        Airflow[Apache Airflow]
    end

    %% Data Source & Ingestion
    subgraph Data_Ingestion ["Ingestion"]
        CSV["Raw CSV Data (2019-Nov.csv)"]
        KafkaProducer[kafka_producer.py]
        KafkaBroker{"Apache Kafka Broker"}
    end

    %% Stream Processing
    subgraph Stream_Processing ["Processing (PySpark)"]
        SparkStream["spark_load.py (Structured Streaming)"]
    end

    %% Data Warehouse
    subgraph Data_Warehouse_Postgres ["Data Warehouse (PostgreSQL)"]
        Bronze[("Bronze: raw_events")]
        Silver[("Silver: silver_products / silver_events")]
        Gold[("Gold: gold_brand_metrics")]
    end

    %% dbt Transformation
    subgraph Transform_dbt ["Transform (dbt)"]
        dbtModels{dbt Models}
    end

    %% Data Flow
    CSV --> KafkaProducer
    KafkaProducer -->|Publishes| KafkaBroker
    KafkaBroker -->|Consumes| SparkStream
    SparkStream -->|JDBC Write| Bronze

    Bronze --> dbtModels
    dbtModels --> Silver
    Silver --> dbtModels
    dbtModels --> Gold

    Airflow -.->|Triggers Task 1| KafkaProducer
    Airflow -.->|Triggers Task 2| dbtModels

    %% Styling
    classDef dbStyle fill:#f9f,stroke:#333,stroke-width:2px;
    classDef scriptStyle fill:#bbf,stroke:#333,stroke-width:1px;
    classDef modelStyle fill:#dfd,stroke:#333,stroke-width:1px;
    classDef dataStyle fill:#eee,stroke:#333,stroke-width:1px;
    classDef messagingStyle fill:#ff9,stroke:#333,stroke-width:2px;
    classDef orchStyle fill:#fdd,stroke:#333,stroke-width:2px;

    class Bronze,Silver,Gold dbStyle;
    class KafkaProducer,SparkStream scriptStyle;
    class dbtModels modelStyle;
    class CSV dataStyle;
    class KafkaBroker messagingStyle;
    class Airflow orchStyle;
```

Getting Started

Follow these steps to replicate the isolated containerized environment and run the pipeline locally.

**1. Prerequisites**
Docker Desktop installed and running.

The raw data file (2019-Nov.csv) placed inside the ./data directory.

**2. Start the Infrastructure**
Spin up the entire stack (PostgreSQL, Kafka, Spark, and custom Airflow image). The --build flag is required to build the custom Airflow image containing necessary dependencies.

docker-compose up -d --build

**3. Start PySpark Streaming**
To consume data from Kafka, the Spark session must be actively listening. Open a new terminal, enter the Spark worker container, and execute the streaming script:

docker exec -it spark_worker bash
python /opt/airflow/project/ingestion/spark_load.py

**4. Access Airflow & Retrieve Password**
During the first startup, Airflow generates a secure standalone admin password. Retrieve it by opening a new terminal tab and running:

docker exec airflow_standalone cat /opt/airflow/standalone_admin_password.txt

Log in to the Airflow UI:

Navigate to http://localhost:8085 in your browser.

Username: admin

Password: (paste the retrieved password string)

**5. Run the Pipeline**
In the Airflow UI, locate the ecommerce_big_data_pipeline DAG.

Toggle the switch on the left to unpause the DAG.

In the Actions column on the right, click the Play button and select Trigger DAG.

Navigate to the Graph view to monitor the Kafka ingestion and dbt transformation tasks in real-time.

**6. View Results**
Once the Airflow tasks complete successfully, connect to the PostgreSQL database via a SQL client (e.g., pgAdmin or DBeaver) using the credentials defined in docker-compose.yml (localhost:5432, user: admin, pass: admin). Query the gold_brand_metrics table to view the final analytical reporting data.

Cleanup
To safely shut down and remove all containers, networks, and images created by this project, run:
docker-compose down
