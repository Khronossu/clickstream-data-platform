import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

warehouse_path = os.path.join(os.getcwd(), "data", "iceberg_warehouse")

# 1. Initialize Spark Session with Kafka + Iceberg dependencies
print("Initializing Spark Session...")
spark = SparkSession.builder \
    .appName("Clickstream-Kafka-to-Bronze") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", warehouse_path) \
    .master("local[*]") \
    .getOrCreate()

# Suppress overly verbose Spark logging
spark.sparkContext.setLogLevel("WARN")

# 2. Define the EXACT Schema (Data Modeling)
# Enforcing this here prevents bad JSON from crashing downstream batch jobs
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("page", StringType(), True),
    StructField("device", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# 3. Read Stream from Kafka
print("Connecting to Kafka...")
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clickstream-events") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Parse JSON and extract fields
# Kafka message values are binary, so we cast to string and parse the JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), event_schema).alias("data")) \
    .select("data.*")

# 5. Advanced Feature: Watermarking & Partitioning Columns
# - Watermarking allows Spark to drop data that arrives more than 10 minutes late, saving state memory.
# - We extract year, month, and day for efficient Parquet partitioning.
enriched_df = parsed_df \
    .withWatermark("timestamp", "10 minutes") \
    .withColumn("year", year("timestamp")) \
    .withColumn("month", month("timestamp")) \
    .withColumn("day", dayofmonth("timestamp"))

# 6. Create the Bronze Iceberg table if it doesn't exist
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.clickstream.bronze (
        event_id STRING,
        user_id STRING,
        session_id STRING,
        event_type STRING,
        page STRING,
        device STRING,
        timestamp TIMESTAMP,
        year INT,
        month INT,
        day INT
    )
    USING iceberg
    PARTITIONED BY (year, month, day)
""")

# 7. Write Stream to Bronze Layer (Iceberg format)
checkpoint_path = os.path.join(os.getcwd(), "data", "checkpoints", "bronze")

print("Starting stream... writing to Iceberg table: local.clickstream.bronze")

query = enriched_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .toTable("local.clickstream.bronze")

# Keep the streaming process alive
query.awaitTermination()