# Clickstream Data Platform

A local-first clickstream pipeline that simulates user traffic, ingests events with Kafka, and lands structured streaming output into a Bronze layer in Parquet.

## Overview

This project demonstrates a Medallion-style data architecture focused on the ingestion and Bronze stages:

1. Generate synthetic clickstream events.
2. Publish events to Kafka.
3. Consume with Spark Structured Streaming.
4. Persist partitioned Parquet files to `data/bronze`.

## Current Status

Implemented now:

1. Kafka + Zookeeper via Docker Compose.
2. Python event producer (`event_generator/producer.py`).
3. Spark streaming ingestion job (`spark/streaming/kafka_to_bronze.py`).
4. Bronze output with checkpointing and date partitioning.

Planned / WIP:

1. Data quality layer.
2. Silver transformations.
3. Gold aggregations.
4. Airflow orchestration.

## Architecture

```text
Event Generator (Python)
  -> Kafka topic: clickstream-events
  -> Spark Structured Streaming
  -> Bronze Lake (Parquet)
```

## Tech Stack

1. Docker + Docker Compose
2. Apache Kafka + Zookeeper
3. PySpark (Structured Streaming)
4. Python 3.11
5. Local filesystem data lake simulation

## Event Schema

Incoming event payloads follow this JSON structure:

```json
{
  "event_id": "UUID",
  "user_id": "string",
  "session_id": "UUID",
  "event_type": "click | view | purchase | scroll",
  "page": "string",
  "device": "mobile | desktop | tablet",
  "timestamp": "ISO-8601 UTC"
}
```

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