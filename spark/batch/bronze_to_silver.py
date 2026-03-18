import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

print("Initializing Spark Batch Job...")
spark = SparkSession.builder \
    .appName("Clickstream-Bronze-to-Silver") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bronze_path = os.path.join(os.getcwd(), "data", "bronze")
silver_path = os.path.join(os.getcwd(), "data", "silver")

# 1. Read the Raw Bronze Data
print(f"Reading raw data from: {bronze_path}")
try:
    bronze_df = spark.read.parquet(bronze_path)
except Exception as e:
    print("Error reading Bronze data. Make sure Step 3 generated parquet files!")
    exit(1)

# 2. CLEAN & PROMOTE TO SILVER LAYER
print("Cleaning data, deduplicating, and dropping bad records...")

# Physical Data Cleaning Operations
silver_df = bronze_df \
    .dropDuplicates(["event_id"]) \
    .dropna(subset=["user_id", "session_id", "timestamp"]) \
    .filter(col("event_type").isin(["click", "view", "purchase", "scroll"]))

# 3. Idempotent Write to Silver
print(f"Writing cleaned data to Silver Layer: {silver_path}")
silver_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(silver_path)

# 4. Human Verification
print("\n=== SAMPLE OF CLEANED SILVER DATA ===")
silver_df.select("user_id", "event_type", "page", "device", "timestamp").show(10, truncate=False)

print("Bronze to Silver pipeline complete.")