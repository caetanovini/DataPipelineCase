import libs.install_dependencies as install_dependencies
from setup.airflow import airflow_setup 
import os
import subprocess
import sys

def trigger_airflow_dag(dag_id):
    print(f"Triggering the Airflow DAG '{dag_id}'...")

    try:
        # Trigger the specified DAG (dag_id) using Airflow CLI
        subprocess.check_call(['airflow', 'dags', 'trigger', dag_id])
        print(f"DAG '{dag_id}' triggered successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error triggering the DAG '{dag_id}': {e}")
        sys.exit(1)

def main():
    try:
        # Airflow setup
        print("Setting up Airflow...")
        airflow_setup.setup_airflow()  # This will handle Airflow setup, starting scheduler and webserver
        print('Airflow setup was successful')

        # Trigger the Airflow DAG 'data_pipeline_dag'
        trigger_airflow_dag('data_pipeline_dag')

    except Exception as e:
        print(f"Error in main process: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
