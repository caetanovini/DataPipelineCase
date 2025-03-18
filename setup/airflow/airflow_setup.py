import subprocess
import sys
import time
import os
import psutil

# Function to execute a system command
def run_command(command):
    try:
        print(f"Executing: {command}")
        subprocess.check_call(command, shell=True)
        print(f"Command executed successfully: {command}")
    except subprocess.CalledProcessError as e:
        print(f"Error executing command {command}. Error: {e}")
        sys.exit(1)

# Function to check if a process is running by name
def is_process_running(process_name):
    for proc in psutil.process_iter(['pid', 'name']):
        if process_name in proc.info['name']:
            return True
    return False

# Function to configure Airflow
def setup_airflow():
    # Set the AIRFLOW_HOME to the project directory
    airflow_home = os.path.expanduser("~/Documents/DataPipelineCase/setup/airflow")
    os.environ["AIRFLOW_HOME"] = airflow_home
    print(f"The AIRFLOW_HOME environment variable is set to: {airflow_home}")

    # Create the default Airflow directories within the project
    os.makedirs(os.path.join(airflow_home, "dags"), exist_ok=True)
    os.makedirs(os.path.join(airflow_home, "logs"), exist_ok=True)
    os.makedirs(os.path.join(airflow_home, "plugins"), exist_ok=True)
    print(f"Airflow directories created at {airflow_home}")

    # Initialize the Airflow database
    print("Initializing the Airflow database...")
    run_command("airflow db init")

    # Create the admin user
    print("Creating the admin user in Airflow...")
    run_command("""
        airflow users create \
            --username admin \
            --firstname admin \
            --lastname admin \
            --role Admin \
            --email admin@example.com \
            --password admin
    """)

    # Start the Airflow scheduler if it's not already running
    if not is_process_running('airflow scheduler'):
        print("Starting the Airflow scheduler...")
        subprocess.Popen("airflow scheduler", shell=True)
        time.sleep(5)  # Wait a moment to ensure the scheduler starts
    else:
        print("Airflow scheduler is already running.")

    # Start the Airflow web server if it's not already running
    if not is_process_running('airflow webserver'):
        print("Starting the Airflow web server...")
        subprocess.Popen("airflow webserver --port 8080", shell=True)
    else:
        print("Airflow web server is already running.")

    print("Airflow is set up and running.")
    print("Access the Airflow web interface at http://localhost:8080")

# Run the Airflow setup
if __name__ == "__main__":
    setup_airflow()
