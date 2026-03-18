import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, count, min, max, unix_timestamp

print("Initializing Spark for Gold Layer Aggregations...")
spark = SparkSession.builder \
    .appName("Clickstream-Silver-to-Gold") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

silver_path = os.path.join(os.getcwd(), "data", "silver")
gold_path = os.path.join(os.getcwd(), "data", "gold")

print(f"Reading clean data from Silver Layer: {silver_path}")
try:
    silver_df = spark.read.parquet(silver_path)
except Exception as e:
    print("Error reading Silver data. Make sure Step 4 completed successfully.")
    exit(1)

print("Calculating Business Metrics...")

# 1. Daily Active Users (DAU)
print("Generating DAU table...")
dau_df = silver_df.groupBy("year", "month", "day") \
    .agg(countDistinct("user_id").alias("daily_active_users")) \
    .orderBy("year", "month", "day")

dau_path = os.path.join(gold_path, "dau")
dau_df.write.mode("overwrite").parquet(dau_path)

# 2. Page Popularity (Views only)
print("Generating Page Popularity table...")
page_views_df = silver_df.filter(col("event_type") == "view") \
    .groupBy("page") \
    .agg(count("*").alias("total_views")) \
    .orderBy(col("total_views").desc())

page_views_path = os.path.join(gold_path, "page_views")
page_views_df.write.mode("overwrite").parquet(page_views_path)

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

session_path = os.path.join(gold_path, "sessions")
session_df.write.mode("overwrite").parquet(session_path)

# Print results to the terminal for verification
print("\n--- GOLD LAYER RESULTS ---")
print("\n1. Daily Active Users:")
dau_df.show(5, truncate=False)

print("\n2. Top Pages by View Count:")
page_views_df.show(5, truncate=False)

print("\n3. Session Metrics (Sample):")
session_df.select("session_id", "events_in_session", "duration_seconds").show(5, truncate=False)

print("\nSilver to Gold pipeline complete.")