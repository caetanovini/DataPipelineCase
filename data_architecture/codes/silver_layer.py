import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

def create_spark_session():
    #JDBC PATH
    jdbc_driver_path = os.path.abspath("libs/sqlite-jdbc-3.36.0.3.jar")

    spark = SparkSession.builder \
        .appName("SilverLayerApp") \
        .config("spark.jars", jdbc_driver_path) \
        .config("spark.driver.extraClassPath", jdbc_driver_path) \
        .config("spark.executor.extraClassPath", jdbc_driver_path) \
        .getOrCreate()
    
    return spark

def transform_to_silver_layer(spark):
    # Absolute path to save the Parquet file
    output_path = os.path.abspath("data_architecture/data_sources/silver_layer.parquet")
    print(f"Saving silver layer data to: {output_path}")
    
    try:
        # Read the raw JSON data using the provided Spark session
        print("Reading the raw JSON data from bronze layer...")
        df = spark.read.json("data_architecture/data_sources/bronze_layer.json")
        print("Data read successfully from bronze layer.")
        
        # Transform data and partition by brewery location (state)
        df_transformed = df\
            .select('id', 'name', 'country', 'state', 'city', col("address_2").alias("village"),
                    'postal_code', 'street', 'latitude', 'longitude', 'phone', 'brewery_type', 'website_url')

        print("Transformation successful. Saving the transformed data...")
        
        # Write the data in Parquet format, partitioned by state
        df_transformed\
            .write\
            .mode("overwrite")\
            .partitionBy("state")\
            .parquet(output_path)
        
        print(f"Silver layer data successfully saved to {output_path}.")
        
    except Exception as e:
        print(f"Error while saving silver layer data: {e}")

if __name__ == "__main__":
    spark = create_spark_session()
    transform_to_silver_layer(spark)
