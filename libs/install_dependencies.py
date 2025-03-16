import subprocess
import sys
import pkg_resources

# Function to install required packages
def install(package):
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        print(f"Successfully installed {package}.")
    except subprocess.CalledProcessError as e:
        print(f"Error installing {package}. Error: {e}")

# Function to check if a package is installed
def is_installed(package):
    installed = {pkg.key for pkg in pkg_resources.working_set}
    return package.lower() in installed

# List of required libraries
required_libraries = ["requests", "pandas", "pyarrow", "pyspark"] #, "apache-airflow", 

# Check and install packages that are not installed
for library in required_libraries:
    if not is_installed(library):
        print(f"{library} is not installed. Installing...")
        install(library)
    else:
        print(f"{library} is already installed.")
