import subprocess
import sys
import os

# Function to install required packages
def install(package):
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        print(f"Successfully installed {package}.")
    except subprocess.CalledProcessError as e:
        print(f"Error installing {package}. Error: {e}")

# Function to check if a package is installed
def is_installed(package):
    try:
        import pkg_resources  # Import here to avoid issues before setuptools installation
        installed = {pkg.key for pkg in pkg_resources.working_set}
        return package.lower() in installed
    except ImportError:
        # If pkg_resources is not available, return False
        return False

# Function to check if Rust and Cargo are installed
def is_rust_installed():
    try:
        subprocess.check_call(["rustc", "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError:
        return False

# Function to install Rust
def install_rust():
    print("Rust is not installed. Installing Rust and Cargo...")
    try:
        subprocess.check_call(["curl", "--proto", "=https", "--tlsv1.2", "-sSf", "https://sh.rustup.rs", "|", "sh"], shell=True)
        subprocess.check_call(["source", "$HOME/.cargo/env"], shell=True)
        print("Successfully installed Rust and Cargo.")
    except subprocess.CalledProcessError as e:
        print(f"Error installing Rust. Error: {e}")

# Function to check if Java is installed (using 'which java' command)
def is_java_installed():
    try:
        subprocess.check_call(["which", "java"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError:
        return False

# Function to install Java (OpenJDK 11 as an example)
def install_java():
    print("Java is not installed. Installing OpenJDK 11...")
    try:
        subprocess.check_call(["sudo", "apt-get", "update"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.check_call(["sudo", "apt-get", "install", "-y", "openjdk-11-jdk"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("Successfully installed Java (OpenJDK 11).")
        
        # After installation, set JAVA_HOME environment variable
        java_home = "/usr/lib/jvm/java-11-openjdk-amd64"
        
        # Set JAVA_HOME environment variable
        os.environ["JAVA_HOME"] = java_home
        os.environ["PATH"] = f"{java_home}/bin:" + os.environ["PATH"]
        print(f"JAVA_HOME is set to {java_home}")
        
    except subprocess.CalledProcessError as e:
        print(f"Error installing Java. Error: {e}")

# Install setuptools if not installed
if not is_installed("setuptools"):
    print("setuptools is not installed. Installing...")
    install("setuptools")

# Update dependencies for Airflow
def update_dependencies():
    # Update pendulum to the latest version
    print("Updating pendulum to the latest version...")
    install("pendulum")

    # Ensure Airflow is installed with the correct version and update it
    print("Updating apache-airflow to the latest compatible version...")
    install("apache-airflow==2.9.0")

    # Install flask-appbuilder 4.5.3 (required by some Airflow providers)
    print("Installing flask-appbuilder==4.5.3...")
    install("flask-appbuilder==4.5.3")

# Update dependencies for Airflow
update_dependencies()

# Check if Rust is installed -> Package necessary to install apache-airflow
if not is_rust_installed():
    install_rust()  # Install Rust if not installed

# Check if Java is installed
if not is_java_installed():
    install_java()  # Install Java if not installed

# List of required libraries
required_libraries = ["requests", "pandas", "pyarrow", "pyspark", "apache-airflow[cncf.kubernetes]"]

# Check and install packages that are not installed
for library in required_libraries:
    if not is_installed(library):
        print(f"{library} is not installed. Installing...")
        install(library)
    else:
        print(f"{library} is already installed.")
