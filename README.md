# Data Engineering Pipeline - Breweries Case 🎲🍺

## 1. Objective 🎯

The goal of this project is to **build a data pipeline** for a brewery dataset following the **medallion architecture**.
The solution includes ingesting data from an API, orchestrating the pipeline in Apache Airflow, and transforming the data using PySpark.

## 2. Data Lake Architecture (Medallion Architecture) 📐

The data lake architecture follows the **Medallion Architecture** model, which consists of three layers:

### **Bronze Layer** 🥉
In this layer, raw data is extracted from the API in its original format (JSON) and no transformations are applied in this layer.

### **Silver Layer** 🥈
In the Silver layer, the raw data is transformed into a **Parquet** columnar storage format. This transformation includes:
- Converting raw data into structured columns for better querying and analysis.
- Partitioning the data by brewery state **location**.

### **Gold Layer** 🥇
In the Gold layer, the data is aggregated, cleaned, and prepared for analysis. 
The data that was stored in **Parquet** format in the Silver layer is now read, aggregated, and stored in a **SQLite** relational database. This enables efficient querying and analysis of the data.
The transformation includes:
- Reading the **Parquet** files from the Silver layer.
- Aggregating the data by **brewery type** and **location**.
- Storing the aggregated data in an **SQLite** database for easy access and analysis.

## 3. Technologies Used 💻

- **API**: [Open Brewery DB API](https://api.openbrewerydb.org/breweries) - A public API used to fetch brewery data.
- **Orchestration**: Apache Airflow.
- **Programming Language**: Python 3.9.
- **Operating System**: Linux (Ubuntu).
- **Storage**: 
  - **Bronze and Silver Layers**: Data from the API is stored locally on the machine where the code is executed, using **Parquet** format for the Silver layer and raw data in JSON format for the Bronze layer.
  - **Gold Layer**: The aggregated and transformed data is stored in a **SQLite** database, allowing efficient querying and analysis.
- **Monitoring/Alerting**: 
  - **Airflow Monitoring**: Apache Airflow provides built-in monitoring tools to track the status of each task in the pipeline, sending email alerts in case of failures.
  - **Data Quality Checks**: Data integrity and validation checks are implemented at each layer to ensure the quality of data as it moves through the pipeline.

## 4. Dependencies 📦
This script is designed to work on **Linux** systems. 
If you're using another operating system (such as **Windows** or **Mac**), you'll need to manually install some dependencies, as outlined below:
- [Apache Spark](https://spark.apache.org/downloads.html)
- [Java JDK](https://www.oracle.com/java/technologies/downloads/)
- [Python](https://www.python.org/downloads/) -> certify that you need to install the 3.9 version.

## 5. Code Structure 📂

The project is organized into several folders to separate different aspects of the pipeline. Here’s a breakdown of the folder structure:

```bash
project_root/
│
├── main.py                       # Entry point for executing the pipeline
├── README.md                     # Project documentation
│
├── data_architecture/            # Contains the data processing architecture
│   ├── codes/                    # Scripts for processing different layers
│   │   ├── bronze_layer.py       # Logic for the Bronze layer (raw data)
│   │   ├── silver_layer.py       # Logic for the Silver layer (transformed data)
│   │   └── golden_layer.py       # Logic for the Gold layer (aggregated data)
│   ├── data_sources/             # Raw and processed data sources
│   │   ├── brewery_data.db       # SQLite database storing aggregated data (Gold layer)
│   │   ├── bronze_layer.json     # Raw JSON data from the API (Bronze layer)
│   │   └── silver_layer.parquet  # Processed data in Parquet format (Silver layer)
│
├── data_base_exploration/        # Data exploration and analysis
│   └── database_insights.ipynb   # Jupyter notebook for exploring and analyzing the database
│
├── libs/                         # Utility scripts and additional resources
│   ├── install_dependencies.py   # Script to install required dependencies (Linux only)
│   └── sqlite-jdbc-3.36.0.3.jar  # SQLite JDBC driver
│
├── setup/                        # Airflow setup and configurations
│   ├── airflow/                  # Airflow configuration files
│   │   ├── airflow_setup.py      # Script to set up Airflow environment
│   │   └── dags/                 # Airflow DAGs
│   │       └── data_pipeline_dag.py  # DAG to orchestrate the data pipeline
│
```

The main goal of this project is to run the `main.py` script, which will set up and execute the entire data pipeline. Below is an overview of how the code is structured and how it works.
The execution of the pipeline begins with the `main.py` file. This script performs the following tasks:

a. **Install Dependencies**: It first ensures that all the necessary dependencies are installed. If they are not already installed, it will use the `install_dependencies.py` script to automatically install the required packages, such as **PySpark**, **Apache Airflow**, and other Python libraries.
   
b. **Airflow Environment Setup**: After installing dependencies, it configures the Apache Airflow environment. This includes setting up the necessary configurations, initializing the Airflow database, and ensuring that all Airflow components are ready to run.

c. **Start Airflow Scheduler and Web Server**: The script will also start the **Airflow Scheduler** and **Web Server** to allow you to monitor and manage the execution of tasks.

d. **Execute the DAG**: After setting up Airflow, it will trigger the execution of the **DAG** (Directed Acyclic Graph), which contains the tasks for extracting, transforming, and loading the data.
## 6. How to Build the Environment and Run the Code 🛠️

### a. Create a Virtual Environment 🐍
To isolate the dependencies for the project, it's recommended to create a virtual environment. This ensures that the project's dependencies don't interfere with your global Python installation.

#### If you're using **Anaconda**:
You can create a new environment with **Python 3.9** by running the following command:

```bash
conda create --name data_pipeline_case python=3.9
```
Once the environment is created, activate it with:

```bash
conda activate data_pipeline_case
```

#### If you're NOT using Anaconda:
If you're not using Anaconda, you can create a virtual environment using venv (included by default in Python 3.3 and later) or virtualenv (if installed).
Make sure you specify *Python 3.9* while creating the environment:

```bash
python3.9 -m venv data_pipeline_case
```
Activate the environment with:
*On Linux/macOS*:
```bash
source data_pipeline_case/bin/activate
```
*On Windows*:
```bash
data_pipeline_case\Scripts\activate
```

### b. Clone the Git Repository 📥
Next, clone the repository to your local machine. You can do this using the following command:
```bash
git clone https://github.com/caetanovini/DataPipelineCase.git
```
### c. Run the `main.py` to Execute the Data Pipeline 🚀
Once you've set up the project, installed the dependencies, and cloned the repository, the next step is to run the **main.py** file. This file is the entry point to execute the data pipeline and will handle the orchestration and execution of the process.

Run the following command:

```bash
python main.py
```

## 7. Data Insights and Data Cleaning 🔍🐞

In the process of analyzing the data from the API, some interesting insights were discovered that led to the cleaning and modification of certain columns. Specifically, five columns—**address_1**, **address_2**, **address_3**, **state_province**, and **state**—had irregular patterns in the data. The following observations were made:

- **address_3**:
  - This column contained only null values across all records.
  - As a result, all values in this column were removed during the data cleaning process.

- **address_1**:
  - It was found that the data in the **address_1** column were exactly the same as the data in the **street** column.
  - To avoid redundancy and maintain a clean dataset, the **address_1** column was removed.

- **address_2**:
  - The **address_2** column contained only one non-null value, which, after further investigation, was identified as information about a **village**.
  - To provide a more meaningful and specific name for the data, the **address_2** column was renamed to **village**.

- **state_province** and **state**:
  - It was observed that the **state_province** column had the same values as the **state** column for all records.
  - To avoid redundancy, the **state_province** column was removed.

### After cleaning and modifying these columns, the next step was to reorder the remaining columns to make the dataset more intuitive and easier to work with. The new column order is as follows:

```python
df_new = df\
    .select('id', 'name', 'country', 'state', 'city', col("address_2").alias("village"),
            'postal_code', 'street','latitude', 'longitude','phone','brewery_type','website_url')
```

### Partitioning Strategy for the Data 📊

During the data analysis process, it became clear that partitioning the data would improve performance and make the data easier to query. The decision on how to partition the data was based on the characteristics of the dataset.

#### Observations:
- After inspecting a sample of 50 records, it was noted that **49 records** were from breweries in the **United States**, while **1 record** was from **Ireland**.
  
  The initial assumption was to partition the data by **country**, but this would result in only two partitions: one for the United States and one for Ireland. Given that the United States contained almost all the data, partitioning by country would not provide significant benefits in terms of performance and data organization.

- Upon further analysis, it became clear that partitioning by **state** would be a better approach, as it would create partitions based on the location within the United States.
- The state field showed diverse values, and partitioning by state would allow for more efficient data access and processing.

##### The file `database_insights.ipynb` contains the full exploration and analysis of the data, as well as the rationale behind each of the steps documented in this section. If you have any questions or doubts about any of the steps, it's recommended to refer to this notebook for a more detailed explanation and understanding of the process.

## 8. Error Handling and Monitoring ⚠️

To ensure that any errors during the execution of the pipeline are properly captured and communicated, several error handling mechanisms were implemented throughout the code.

### Error Handling in the Code

In the Python code, **`try`** and **`except`** blocks were used extensively to catch and handle potential errors that may arise during the execution of the data pipeline. This allows the user to identify the exact location and nature of any issues that occur, making debugging easier. 

For example:

```python
try:
    # Some code that may raise an exception
    result = some_operation()
except Exception as e:
    print(f"An error occurred: {e}")
    # Additional handling if needed
```

### Error Handling in Airflow
For monitoring errors in the Airflow DAG, a set of default arguments were configured to ensure that, in case of a failure, the system will send an email notification with details about the error. 
The following settings were applied in the DAG configuration:

```python
# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,  # Retry once if a task fails
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes before retrying
    'email_on_failure': True,  # Sends an email in case of failure
    'email_on_retry': False,  # Does not send email on retry
    'email': ['admin@admin.com']  # Replace with your email for notifications
}
```
With these settings, if a task in the Airflow DAG fails, an email will be sent to the specified address (admin@admin.com in this case, which should be replaced with a valid email address) with details about the failure and the logs from Airflow.
This helps in quickly identifying any issues and taking action to resolve them.



