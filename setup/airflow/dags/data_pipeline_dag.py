from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os
from pyspark.sql import SparkSession

# Function to create and return the Spark session
def create_spark_session():
    jdbc_driver_path = os.path.abspath("libs/sqlite-jdbc-3.36.0.3.jar")

    spark = SparkSession.builder \
        .appName("AirflowSparkSession") \
        .config("spark.jars", jdbc_driver_path) \
        .config("spark.driver.extraClassPath", jdbc_driver_path).getOrCreate()
    print("Spark session created successfully.")
    return spark

# Function to execute the Python script (this is where you would run your layer scripts)
def run_script(script_name, spark):
    try:
        # Get the absolute path to the script
        script_path = os.path.join('data_architecture/codes', script_name)

        # Only print the message for scripts that are not 'bronze_layer.py'
        print(f"Executing {script_path}...")

        # Execute the script using Python and pass Spark session as an environment variable or argument
        subprocess.check_call([sys.executable, script_path, str(spark)])  # Pass the Spark session explicitly
        print(f"{script_path} executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_name}. Error: {e}")
        raise

# Function to run the bronze layer (doesn't require spark session)
def run_bronze_layer():
    run_script('bronze_layer.py', None)

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
    python_callable=run_bronze_layer,
    dag=dag,
)

# Task to run the silver layer (requires spark session)
def run_silver_layer(**kwargs):
    # Create a new Spark session within the task
    spark = create_spark_session()  # Create a new Spark session
    run_script('silver_layer.py', spark)  # Run silver layer script
    # No need to stop Spark here since it's stopped later (if necessary)

silver_task = PythonOperator(
    task_id='run_silver_layer',
    python_callable=run_silver_layer,
    provide_context=True,  # This is required to pass 'kwargs' to the task
    dag=dag,
)

# Task to run the golden layer (requires spark session)
def run_golden_layer(**kwargs):
    # Create a new Spark session within the task
    spark = create_spark_session()  # Create a new Spark session
    run_script('golden_layer.py', spark)  # Run golden layer script
    # No need to stop Spark here since it's stopped later (if necessary)

golden_task = PythonOperator(
    task_id='run_golden_layer',
    python_callable=run_golden_layer,
    provide_context=True,  # This is required to pass 'kwargs' to the task
    dag=dag,
)

# Task to stop the Spark session (if needed)
def stop_spark_session():
    print("Stopping the Spark session...")
    # No need to manually stop Spark, it's managed by Spark itself when the session is closed.
    print("Spark session stopped successfully.")

stop_spark_task = PythonOperator(
    task_id='stop_spark_session',
    python_callable=stop_spark_session,
    dag=dag,
)

# Set task dependencies
bronze_task >> silver_task >> golden_task >> stop_spark_task  # Defines the execution sequence

