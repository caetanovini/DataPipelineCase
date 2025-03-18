from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os
import logging

# Function to execute the Python script (bronze, silver, etc.)
def run_script(script_name):  # Adding **kwargs to catch Airflow's extra arguments
    try:
        # Absolute path to the script
        script_path = os.path.join('data_architecture/codes', script_name)
        
        # Check if the file exists
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Script not found: {script_path}")
        
        # Log the start of script execution
        logging.info(f"Starting execution of script: {script_name}")
        
        # Execute the script using Python
        subprocess.check_call([sys.executable, script_path])  # Spark session is handled inside the script
        logging.info(f"Script {script_name} executed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing {script_name}. Error: {e}")
        raise  # Re-raise the error so that Airflow can detect it
    except FileNotFoundError as e:
        logging.error(f"Error: {e}")
        raise  # Re-raise the error
    except Exception as e:
        logging.error(f"Unknown error executing {script_name}: {e}")
        raise  # Re-raise the error

# Functions to run the layers (bronze, silver, and golden)
def run_bronze_layer():
    run_script('bronze_layer.py')

def run_silver_layer():
    run_script('silver_layer.py')

def run_golden_layer():
    run_script('golden_layer.py')

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,  # Sends an email in case of failure
    'email_on_retry': False,  # Does not send email on retry
    'email': ['admin@admin.com']  # Replace with your email for notifications
}

# Define the DAG
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

# Task to run the silver layer
silver_task = PythonOperator(
    task_id='run_silver_layer',
    python_callable=run_silver_layer,  
    dag=dag,
)

# Task to run the golden layer
golden_task = PythonOperator(
    task_id='run_golden_layer',
    python_callable=run_golden_layer, 
    dag=dag,
)

# Set task dependencies
bronze_task >> silver_task >> golden_task  # Defines the execution order of the tasks
