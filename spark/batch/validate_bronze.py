import os
import json
from pyspark.sql import SparkSession
from great_expectations.dataset import SparkDFDataset

print("Initializing Spark for Data Validation...")
spark = SparkSession.builder \
    .appName("Bronze-Data-Validation") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bronze_path = os.path.join(os.getcwd(), "data", "bronze")
print(f"Reading raw Bronze data from: {bronze_path}\n")

try:
    bronze_df = spark.read.parquet(bronze_path)
except Exception as e:
    print(f"Error reading Bronze data: {e}")
    exit(1)

# Wrap the PySpark DataFrame in a Great Expectations wrapper
ge_df = SparkDFDataset(bronze_df)

print("RUNNING DATA QUALITY GATES...\n")

# Gate 1: Completeness
res_null = ge_df.expect_column_values_to_not_be_null("user_id")
print(f"[{'PASS' if res_null['success'] else 'FAIL'}] user_id column has no null values.")

# Gate 2: Validity
allowed_events = ["click", "view", "purchase", "scroll"]
res_events = ge_df.expect_column_values_to_be_in_set("event_type", allowed_events)
print(f"[{'PASS' if res_events['success'] else 'FAIL'}] event_type contains only allowed values.")

# Gate 3: Uniqueness
res_unique = ge_df.expect_column_values_to_be_unique("event_id")
print(f"[{'PASS' if res_unique['success'] else 'FAIL'}] event_id values are 100% unique.")

print("\nDETAILED METRICS (Event Types):")
# Great Expectations gives us deep statistical profiling for free
print(json.dumps(res_events["result"], indent=2))