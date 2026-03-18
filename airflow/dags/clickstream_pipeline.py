from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Define default arguments
default_args = {
    'owner': 'Purin',
    'depends_on_past': False,
    # Start date is set in the past so Airflow can trigger it immediately
    'start_date': datetime(2026, 3, 18),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

# 2. Instantiate the DAG
with DAG(
    dag_id='clickstream_medallion_pipeline',
    default_args=default_args,
    description='Orchestrates the batch processing from Bronze to Gold',
    schedule_interval='@hourly',
    catchup=False, # Crucial: prevents running hundreds of historical jobs at once
    tags=['clickstream', 'pyspark'],
) as dag:

    # 3. Define the Tasks
    # We use /opt/airflow/ because that is how we mapped your local folders 
    # into the Docker container in the docker-compose.yml
    
    run_bronze_to_silver = BashOperator(
        task_id='bronze_to_silver_cleaning',
        bash_command='python /opt/airflow/spark/batch/bronze_to_silver.py',
        cwd='/opt/airflow'
    )

    run_silver_to_gold = BashOperator(
        task_id='silver_to_gold_aggregations',
        bash_command='python /opt/airflow/spark/batch/silver_to_gold.py',
        cwd='/opt/airflow'
    )

    # 4. Define the Execution Order
    # Task 1 MUST succeed before Task 2 starts
    run_bronze_to_silver >> run_silver_to_gold