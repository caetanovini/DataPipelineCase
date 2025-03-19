# Data Engineering Pipeline - Breweries Case 🎲🍺

## Objective 🎯

The goal of this project is to build a data pipeline for a brewery dataset following the medallion architecture with three layers: bronze, silver, and gold. 
The solution includes ingesting data from an API, orchestrating the pipeline in Apache Airflow, and transforming the data using PySpark.

## Technologies Used 💻

- **API**: [Open Brewery DB API](https://api.openbrewerydb.org/breweries) - A public API used to fetch brewery data.
- **Orchestration**: Apache Airflow - Used for orchestrating the data pipeline, scheduling tasks, handling retries, and managing task dependencies.
- **Programming Language**: Python 3.9 - The code was developed using Python 3.9`.
- **Operating System**: Linux (Ubuntu) - The development and execution environment for this project is Linux (Ubuntu). All dependencies and tools were installed and run on an Ubuntu system.
- **Storage**: 
  - **Bronze and Silver Layers**: Data from the API is stored locally on the machine where the code is executed, using **Parquet** format for the Silver layer and raw data in JSON format for the Bronze layer.
  - **Gold Layer**: The aggregated and transformed data is stored in a **SQLite** database, allowing efficient querying and analysis.
- **Monitoring/Alerting**: 
  - **Airflow Monitoring**: Apache Airflow provides built-in monitoring tools to track the status of each task in the pipeline, sending email alerts in case of failures.
  - **Data Quality Checks**: Data integrity and validation checks are implemented at each layer to ensure the quality of data as it moves through the pipeline.


## Data Lake Architecture (Medallion Architecture) 📐

The data lake architecture follows the **Medallion Architecture** model, which consists of three layers:

### 1. **Bronze Layer** 🥉
In this layer, raw data is extracted from the API in its original format (JSON) and no transformations are applied in this layer.

### 2. **Silver Layer** 🥈
In the Silver layer, the raw data is transformed into a **Parquet** columnar storage format. This transformation includes:
- Converting raw data into structured columns for better querying and analysis.
- Partitioning the data by brewery state **location**.

### 3. **Gold Layer** 🥇
In the Gold layer, the data is aggregated, cleaned, and prepared for analysis. 
The data that was stored in **Parquet** format in the Silver layer is now read, aggregated, and stored in a **SQLite** relational database. This enables efficient querying and analysis of the data.
The transformation includes:
- Reading the **Parquet** files from the Silver layer.
- Aggregating the data by **brewery type** and **location**.
- Storing the aggregated data in an **SQLite** database for easy access and analysis.
