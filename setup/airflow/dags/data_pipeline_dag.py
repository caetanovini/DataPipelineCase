from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

# Function to execute the Python script (this is where you would run your layer scripts)
def run_script(script_name):
    try:
        # Get the absolute path to the script
        script_path = os.path.join('data_architecture/codes', script_name)

        # Execute the script using Python
        subprocess.check_call([sys.executable, script_path])  # No need to pass Spark session, it's handled inside the script
        print(f"{script_path} executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_name}. Error: {e}")
        raise

# Define the DAG with appropriate arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline_dag',
    default_args=default_args,
    description='DAG to process bronze, silver, and golden layers',
    schedule_interval=None,  # Set to 'None' for manual execution
    start_date=datetime(2025, 3, 16),  # Start date of execution
    catchup=False,  # Do not execute past DAG runs
)

# Task to run the bronze layer (doesn't require spark session)
bronze_task = PythonOperator(
    task_id='run_bronze_layer',
    python_callable=run_script('bronze_layer.py'),
    dag=dag,
)

# Task to run the silver layer (requires spark session)
silver_task = PythonOperator(
    task_id='run_silver_layer',
    python_callable=run_script('silver_layer.py'),
    dag=dag,
)

# Task to run the golden layer (requires spark session)
golden_task = PythonOperator(
    task_id='run_golden_layer',
    python_callable=run_script('golden_layer.py'),
    dag=dag,
)

# Set task dependencies
bronze_task >> silver_task >> golden_task  # Defines the execution sequence
