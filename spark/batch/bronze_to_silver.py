import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from great_expectations.dataset import SparkDFDataset

warehouse_path = os.path.join(os.getcwd(), "data", "iceberg_warehouse")

print("Initializing Configuration-Driven Batch Job...")
spark = SparkSession.builder \
    .appName("Clickstream-Bronze-to-Silver-Dynamic") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", warehouse_path) \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define Paths
base_dir = os.getcwd()
rules_path = os.path.join(base_dir, "data_quality", "expectations", "clickstream_suite.json")

# 1. Read the Raw Bronze Data from Iceberg
print("Reading raw data from Iceberg table: local.clickstream.bronze")

try:
    bronze_df = spark.table("local.clickstream.bronze")
except Exception as e:
    print(f"Error reading Bronze data: {e}")
    exit(1)

# 2. Load the Data Contract (JSON)
print(f"Loading data quality rules from: {rules_path}")
with open(rules_path, 'r') as f:
    suite = json.load(f)

# 3. DYNAMIC VALIDATION (Great Expectations)
print("\n--- RUNNING DYNAMIC VALIDATION ---")
ge_df = SparkDFDataset(bronze_df)
validation_passed = True

for expectation in suite["expectations"]:
    rule_name = expectation["expectation_type"]
    kwargs = expectation["kwargs"]
    
    # Python magic: dynamically call the Great Expectations method by its string name
    validation_method = getattr(ge_df, rule_name)
    result = validation_method(**kwargs)
    
    status = "PASS" if result["success"] else "FAIL"
    print(f"[{status}] Rule: {rule_name} | Target: {kwargs.get('column')}")
    
    if not result["success"]:
        validation_passed = False

if not validation_passed:
    print("WARNING: Some data quality checks failed. Proceeding with aggressive cleaning.")

# 4. DYNAMIC CLEANING (PySpark)
print("\n--- APPLYING DYNAMIC CLEANING ---")
silver_df = bronze_df

# Translate the JSON rules into PySpark DataFrame operations
for expectation in suite["expectations"]:
    rule_name = expectation["expectation_type"]
    kwargs = expectation["kwargs"]
    column = kwargs.get("column")
    
    if rule_name == "expect_column_values_to_not_be_null":
        print(f"-> Dropping nulls in {column}")
        silver_df = silver_df.dropna(subset=[column])
        
    elif rule_name == "expect_column_values_to_be_unique":
        print(f"-> Deduplicating based on {column}")
        silver_df = silver_df.dropDuplicates([column])
        
    elif rule_name == "expect_column_values_to_be_in_set":
        value_set = kwargs.get("value_set")
        print(f"-> Filtering {column} to allowed values: {value_set}")
        silver_df = silver_df.filter(col(column).isin(value_set))

# 5. Idempotent Write to Silver (Iceberg)
print("\nWriting cleaned data to Iceberg table: local.clickstream.silver")
silver_df.writeTo("local.clickstream.silver") \
    .using("iceberg") \
    .partitionedBy("year", "month", "day") \
    .createOrReplace()

print("\n=== SAMPLE OF CLEANED SILVER DATA ===")
silver_df.select("user_id", "event_type", "page", "device", "timestamp").show(10, truncate=False)

print("Bronze to Silver pipeline complete.")