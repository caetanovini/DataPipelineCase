import os
from pyspark.sql import SparkSession
from pyspark import SparkConf

def create_spark_session():
    # Path to the JDBC driver
    jdbc_driver_path = os.path.abspath("libs/sqlite-jdbc-3.36.0.3.jar")

    # SparkConf setup for configuring the Spark session
    conf = SparkConf() \
        .set("spark.jars", jdbc_driver_path) \
        .set("spark.driver.extraClassPath", jdbc_driver_path) \
        .set("spark.executor.extraClassPath", jdbc_driver_path) \
        .set("spark.sql.catalogImplementation", "in-memory") \
        .set("spark.sql.shuffle.partitions", "1")

    # Create or get the Spark session with the specified configuration
    spark = SparkSession.builder \
        .appName("GoldenLayerApp") \
        .config(conf=conf) \
        .getOrCreate()
    
    return spark

def transform_to_gold_layer(spark):
    try:
        # Define the JDBC URL for SQLite
        jdbc_url = "jdbc:sqlite:" + os.path.abspath("data_architecture/data_sources/brewery_data.db")
        table_name = "golden_layer"
        
        # Force Spark to load the JDBC driver
        spark._jvm.org.sqlite.JDBC  # Ensure the JDBC driver is explicitly loaded

        print(f"SQLite database path: {jdbc_url}")

        # Read data from the silver layer (Parquet format)
        silver_df = spark.read.parquet('data_architecture/data_sources/silver_layer.parquet')
        print("Data successfully read from the silver layer.")

        # Aggregate the data
        aggregated_df = silver_df.groupBy("state", "brewery_type").count()

        # Rename the count column to 'brewery_quantity'
        golden_df = aggregated_df.withColumnRenamed('count', 'brewery_quantity')

        print(f"Writing gold layer data to SQLite database at: {jdbc_url}...")

        # Repartition the data to a single partition before saving
        golden_df_repartitioned = golden_df.repartition(1)

        # Write the data to the SQLite database
        golden_df_repartitioned.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("driver", "org.sqlite.JDBC") \
            .mode("overwrite").save()

        print(f"Data successfully written to SQLite table: {table_name}")
    except Exception as e:
        print(f"Error writing to SQLite: {e}")

if __name__ == "__main__":
    # Create the Spark session inside the layer script
    spark = create_spark_session()  
    transform_to_gold_layer(spark)
    spark.stop()
