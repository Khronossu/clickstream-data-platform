import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

print("Booting up BI Dashboard Engine...\n")
spark = SparkSession.builder \
    .appName("Clickstream-Analytics-Dashboard") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

gold_path = os.path.join(os.getcwd(), "data", "gold")

def print_header(title):
    print(f"\n{'='*40}")
    print(f" 📊 {title} 📊")
    print(f"{'='*40}")

try:
    # --- METRIC 1: Daily Active Users ---
    print_header("DAILY ACTIVE USERS (DAU)")
    dau_df = spark.read.parquet(os.path.join(gold_path, "dau"))
    # Format nicely for the terminal
    dau_df.orderBy(col("year").desc(), col("month").desc(), col("day").desc()).show(5, truncate=False)

    # --- METRIC 2: Page Popularity ---
    print_header("TOP PAGES BY TRAFFIC")
    page_df = spark.read.parquet(os.path.join(gold_path, "page_views"))
    page_df.orderBy(col("total_views").desc()).show(5, truncate=False)

    # --- METRIC 3: User Engagement ---
    print_header("USER ENGAGEMENT (Session Duration)")
    session_df = spark.read.parquet(os.path.join(gold_path, "sessions"))
    
    # Calculate average session length
    session_df.createOrReplaceTempView("sessions")
    avg_duration = spark.sql("""
        SELECT 
            COUNT(session_id) as total_sessions,
            ROUND(AVG(duration_seconds), 2) as avg_session_length_seconds,
            MAX(events_in_session) as max_clicks_in_one_session
        FROM sessions
    """)
    avg_duration.show(truncate=False)

except Exception as e:
    print(f"\n⚠️  No Gold data found yet! Make sure your Airflow DAG has run successfully.")
    print(f"Error details: {e}")

print("\nEnd of Report.")