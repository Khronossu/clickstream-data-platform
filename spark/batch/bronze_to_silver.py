import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from great_expectations.dataset import SparkDFDataset

# 1. Initialize Spark Session for Batch Processing
print("Initializing Spark Batch Job...")
spark = SparkSession.builder \
    .appName("Clickstream-Bronze-to-Silver") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bronze_path = os.path.join(os.getcwd(), "data", "bronze")
silver_path = os.path.join(os.getcwd(), "data", "silver")

# 2. Read the Raw Bronze Data
print(f"Reading raw data from: {bronze_path}")
try:
    bronze_df = spark.read.parquet(bronze_path)
except Exception as e:
    print("Error reading Bronze data. Make sure Step 3 has generated parquet files!")
    exit(1)

# ==========================================
# 3. DATA QUALITY GATES (Great Expectations)
# ==========================================
print("Running Great Expectations Data Quality Checks...")
ge_df = SparkDFDataset(bronze_df)

# Define our strict rules
results = {
    "user_id_not_null": ge_df.expect_column_values_to_not_be_null("user_id"),
    "event_id_unique": ge_df.expect_column_values_to_be_unique("event_id"),
    "valid_event_types": ge_df.expect_column_values_to_be_in_set(
        "event_type", ["click", "view", "purchase", "scroll"]
    )
}

# Evaluate the results
failed_expectations = [name for name, res in results.items() if not res["success"]]

if failed_expectations:
    print(f"WARNING: Data Quality checks failed for: {failed_expectations}")
    # In a production Airflow DAG, you would raise an Exception here to stop the pipeline and alert Slack.
    # For this project, we will log the warning and proceed to clean the data.
else:
    print("All Data Quality checks passed!")

# ==========================================
# 4. CLEAN & PROMOTE TO SILVER LAYER
# ==========================================
print("Cleaning data and dropping bad records...")

# Physical Data Cleaning Operations
silver_df = bronze_df \
    .dropDuplicates(["event_id"]) \
    .dropna(subset=["user_id", "session_id", "timestamp"]) \
    .filter(col("event_type").isin(["click", "view", "purchase", "scroll"]))

# 5. Idempotent Write to Silver
print(f"Writing cleaned data to Silver Layer: {silver_path}")
silver_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(silver_path)

print("Bronze to Silver pipeline complete!")