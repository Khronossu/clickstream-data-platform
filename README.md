# End-to-End Clickstream Data Platform (Medallion Architecture)

## Project Overview

This project is a fully containerized, distributed data pipeline designed to ingest, process, and aggregate real-time clickstream data. Built around the __Medallion Architecture__ (Bronze, Silver, Gold), the platform simulates a high-throughput e-commerce environment, processing raw user events into actionable business intelligence metrics.

The infrastructure is heavily decoupled, utilizing __Kafka__ for real-time message brokering, __PySpark Structured Streaming__ for continuous ingestion, and __Apache Airflow__ for orchestrating idempotent batch transformations and data quality checks.

## Repository Layout

```text
.
|- docker-compose.yaml
|- Dockerfile
|- airflow/
|  |- dag/
|     |- dags/
|        |- clickstream_pipeline.py
|- event_generator/
|  |- producer.py
|- spark/
|  |- streaming/
|     |- kafka_to_bronze.py
|  |- batch/
|     |- bronze_to_silver.py
|     |- silver_to_gold.py
|     |- validate_bronze.py
|- data/
|  |- bronze/
|  |- silver/
|  |- gold/
|  |- checkpoints/
|- data_quality/
|  |- expectations/
|     |- clickstream_suite.json
```


## Architecture Flow

1. __Event Generation:__ A Python producer simulates live e-commerce web traffic, generating JSON payloads (User IDs, Page Views, Timestamps) and streaming them to a Kafka topic.
2. __Bronze Layer (Raw Ingestion):__ A PySpark Structured Streaming job consumes the Kafka topic in real-time, checkpointing the offsets and writing the raw data as partitioned Parquet files (`/year/month/day`).
3. __Silver Layer (Cleansed & Validated):__ An Airflow-orchestrated PySpark batch job reads the Bronze data. It utilizes __Great Expectations__ to enforce strict data contracts (null checks, deduplication, schema validation) before performing idempotent overwrites into the Silver layer.
4. __Gold Layer (Business Aggregations):__ A final Airflow task aggregates the cleansed Silver data into business-critical metrics (Daily Active Users, Top Pages by Traffic, Session Analytics) for downstream BI consumption.

## Tech Stack & Engineering Highlights

* __Data Processing:__ PySpark (3.5.0), Python 3.11

* __Streaming & Messaging:__ Apache Kafka, Zookeeper

* __Orchestration:__ Apache Airflow (2.8.1) running on LocalExecutor

* __Data Quality:__ Great Expectations (Data Contracts & JSON Suites)

* __Infrastructure:__ Docker & Docker Compose (Custom built for cross-platform compatibility, including Apple Silicon/ARM64 support).


### Key Engineering Decisions

* __Fault Tolerance:__ Implemented Spark checkpointing to guarantee exactly-once processing and seamless recovery from stream failures.

* __Idempotency:__ Silver and Gold batch transformations are designed to be fully idempotent, allowing for safe backfilling and DAG retries without data duplication.

* __Data Contracts:__ Shifted data quality to the left by enforcing JSON-based Great Expectations rules before data enters the Silver layer, preventing downstream corruption.


## Prerequisites

1. Docker Desktop (or Docker Engine + Compose plugin)
2. Python 3.11
3. Java 17 (required by Spark/PySpark)

## Quick Start

### 1) Boot the Infrastructure

```bash
docker compose up -d
```

### 2) Start the Live Data Stream
__Terminal A (The Producer):__

```bash
python event_generator/producer.py
```
__Terminal B (The Ingestor):__
```bash
python spark/streaming/kafka_to_bronze.py
```

### 3) Orchestrate the Transformations

1. Navigate to the Airflow UI at `http://localhost:8080` (Default credentials: `admin / admin`).

2. Unpause the `clickstream_medallion_pipeline` DAG.

3. Trigger the DAG manually to process the Bronze data through the Silver and Gold transformations.

### 4) View the Business Dashboard
Once the Airflow tasks succeed, run the analytics engine to view the final Gold layer metrics:
```bash
python query_gold.py
```
#### Sample Output (Gold Layer)
The pipeline successfully generates analytics ready for BI dashboards:
* __Daily Active Users (DAU):__ Tracks unique users interacting with the platform per day.
* __Top Pages by Traffic:__ Ranks the most visited routes (e.g.,` /checkout, /products`).
* __Session Analytics:__ Calculates average session duration and engagement depth.

## Notes

1. Watermarking is set to `10 minutes` in the Spark job.

## License

[MIT](https://choosealicense.com/licenses/mit/)