import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, count, min, max, unix_timestamp

warehouse_path = os.path.join(os.getcwd(), "data", "iceberg_warehouse")

print("Initializing Spark for Gold Layer Aggregations...")
spark = SparkSession.builder \
    .appName("Clickstream-Silver-to-Gold") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", warehouse_path) \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Reading clean data from Iceberg table: local.clickstream.silver")
try:
    silver_df = spark.table("local.clickstream.silver")
except Exception as e:
    print("Error reading Silver data. Make sure Step 4 completed successfully.")
    exit(1)

print("Calculating Business Metrics...")

# 1. Daily Active Users (DAU)
print("Generating DAU table...")
dau_df = silver_df.groupBy("year", "month", "day") \
    .agg(countDistinct("user_id").alias("daily_active_users")) \
    .orderBy("year", "month", "day")

dau_df.writeTo("local.clickstream.gold_dau").using("iceberg").createOrReplace()

# 2. Page Popularity (Views only)
print("Generating Page Popularity table...")
page_views_df = silver_df.filter(col("event_type") == "view") \
    .groupBy("page") \
    .agg(count("*").alias("total_views")) \
    .orderBy(col("total_views").desc())

page_views_df.writeTo("local.clickstream.gold_page_views").using("iceberg").createOrReplace()

# 3. Session Metrics
print("Generating Session Metrics table...")
session_df = silver_df.groupBy("session_id") \
    .agg(
        count("*").alias("events_in_session"),
        min("timestamp").alias("session_start"),
        max("timestamp").alias("session_end")
    ) \
    .withColumn("duration_seconds",
                unix_timestamp("session_end") - unix_timestamp("session_start"))

session_df.writeTo("local.clickstream.gold_sessions").using("iceberg").createOrReplace()

# Print results to the terminal for verification
print("\n--- GOLD LAYER RESULTS ---")
print("\n1. Daily Active Users:")
dau_df.show(5, truncate=False)

print("\n2. Top Pages by View Count:")
page_views_df.show(5, truncate=False)

print("\n3. Session Metrics (Sample):")
session_df.select("session_id", "events_in_session", "duration_seconds").show(5, truncate=False)

print("\nSilver to Gold pipeline complete.")