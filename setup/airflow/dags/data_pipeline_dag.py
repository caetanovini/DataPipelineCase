from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

# Function to execute the Python script (this is where you would run your layer scripts)
def run_script(script_name):  # Add **kwargs to catch Airflow's extra arguments
    try:
        # Get the absolute path to the script
        script_path = os.path.join('data_architecture/codes', script_name)
        
        # Check if the file exists
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Script not found: {script_path}")
        
        # Execute the script using Python
        subprocess.check_call([sys.executable, script_path])  # No need to pass Spark session, it's handled inside the script
        print(f"{script_path} executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_name}. Error: {e}")
        raise
    except FileNotFoundError as e:
        print(f"Error: {e}")
        raise

def run_bronze_layer():
    run_script('bronze_layer.py')

def run_silver_layer():
    run_script('silver_layer.py')

def run_golden_layer():
    run_script('golden_layer.py')

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline_dag',
    default_args=default_args,
    description='DAG to process bronze, silver, and golden layers',
    schedule_interval=None,  # Change to None for testing purposes
    start_date=datetime.now(),
    catchup=False,  # Do not execute past DAG runs
)

# Task to run the bronze layer
bronze_task = PythonOperator(
    task_id='run_bronze_layer',
    python_callable=run_bronze_layer,
    dag=dag,
)

# # Task to run the silver layer
silver_task = PythonOperator(
    task_id='run_silver_layer',
    python_callable=run_silver_layer,  
    dag=dag,
)

# # Task to run the golden layer
golden_task = PythonOperator(
    task_id='run_golden_layer',
    python_callable=run_golden_layer, 
    dag=dag,  # Adicionando o parâmetro dag
)

# Set task dependencies
bronze_task >> silver_task >> golden_task  # Defines the execution sequence
