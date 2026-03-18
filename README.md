# End-to-End Clickstream Data Platform (Medallion Architecture)

## Project Overview

This project is a fully containerized, distributed data pipeline designed to ingest, process, and aggregate real-time clickstream data. Built around the __Medallion Architecture__ (Bronze, Silver, Gold), the platform simulates a high-throughput e-commerce environment, processing raw user events into actionable business intelligence metrics.

The infrastructure is heavily decoupled, utilizing __Kafka__ for real-time message brokering, __PySpark Structured Streaming__ for continuous ingestion, and __Apache Airflow__ for orchestrating idempotent batch transformations and data quality checks.

## Architecture Flow

1. __Event Generation:__ A Python producer simulates live e-commerce web traffic, generating JSON payloads (User IDs, Page Views, Timestamps) and streaming them to a Kafka topic.
2. __Bronze Layer (Raw Ingestion):__ A PySpark Structured Streaming job consumes the Kafka topic in real-time, checkpointing the offsets and writing the raw data as partitioned Parquet files (`/year/month/day`).
3. __Silver Layer (Cleansed & Validated):__ An Airflow-orchestrated PySpark batch job reads the Bronze data. It utilizes __Great Expectations__ to enforce strict data contracts (null checks, deduplication, schema validation) before performing idempotent overwrites into the Silver layer.
4. __Gold Layer (Business Aggregations):__ A final Airflow task aggregates the cleansed Silver data into business-critical metrics (Daily Active Users, Top Pages by Traffic, Session Analytics) for downstream BI consumption.

## Tech Stack & Engineering Highlights

```text
Event Generator (Python)
  -> Kafka topic: clickstream-events
  -> Spark Structured Streaming
  -> Bronze Lake (Parquet)
```

## Tech Stack

* __Data Processing:__ PySpark (3.5.0), Python 3.11

* __Streaming & Messaging:__ Apache Kafka, Zookeeper

* __Orchestration:__ Apache Airflow (2.8.1) running on LocalExecutor

* __Data Quality:__ Great Expectations (Data Contracts & JSON Suites)

* __Infrastructure:__ Docker & Docker Compose (Custom built for cross-platform compatibility, including Apple Silicon/ARM64 support).

## Key Engineering Decisions

* __Fault Tolerance:__ Implemented Spark checkpointing to guarantee exactly-once processing and seamless recovery from stream failures.

* __Idempotency:__ Silver and Gold batch transformations are designed to be fully idempotent, allowing for safe backfilling and DAG retries without data duplication.

* __Data Contracts:__ Shifted data quality to the left by enforcing JSON-based Great Expectations rules before data enters the Silver layer, preventing downstream corruption.

## Repository Layout

```text
.
|- docker-compose.yaml
|- event_generator/
|  |- producer.py
|- spark/
|  |- streaming/
|     |- kafka_to_bronze.py
|- data/
|  |- bronze/
|  |- checkpoints/
```

## Prerequisites

1. Docker Desktop (or Docker Engine + Compose plugin)
2. Python 3.11
3. Java 17 (required by Spark/PySpark)

## Quick Start

### 1) Start Kafka Infrastructure

```bash
docker compose up -d
```

### 2) Create Python Environment

```bash
python3.11 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install pyspark confluent-kafka faker
```

### 3) Run Event Producer

In terminal A:

```bash
python event_generator/producer.py
```

### 4) Run Streaming Consumer (Kafka -> Bronze)

In terminal B:

```bash
python spark/streaming/kafka_to_bronze.py
```

### 5) Verify Bronze Output

```bash
find data/bronze -maxdepth 4 -type d | sort
```

You should see date-partitioned output like:

```text
data/bronze/year=2026/month=3/day=18
```

## Stop and Cleanup

```bash
docker compose down -v
```

To also remove generated local data:

```bash
rm -rf data/bronze data/checkpoints
```

## Notes

1. The Kafka topic `clickstream-events` is auto-created by the current broker config.
2. The streaming job uses `startingOffsets=earliest`, so it can replay existing events.
3. Watermarking is set to `10 minutes` in the Spark job.

## License

[MIT](https://choosealicense.com/licenses/mit/)