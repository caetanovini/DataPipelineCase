import os
from pyspark.sql import SparkSession

# Function to get the Spark session (optional, depending on how you run the DAG)
def get_spark_session_from_args():
    # Create or get the Spark session
    spark = SparkSession.builder.getOrCreate()
    return spark

def transform_to_gold_layer(spark):
    try:
        # Absolute path for the SQLite database file
        jdbc_url = "jdbc:sqlite:" + os.path.abspath("data_architecture/data_sources/brewery_data.db")
        table_name = "golden_layer"  # Name of the table where data will be saved

        # Check if the path for the database is valid
        print(f"SQLite database path: {jdbc_url}")

        # Read the Parquet file from the silver layer
        silver_df = spark.read.parquet('data_architecture/data_sources/silver_layer.parquet')

        print("Data successfully read from the silver layer.")

        # Perform aggregation (group by 'state' and 'brewery_type')
        aggregated_df = silver_df.groupBy("state", "brewery_type").count()

        # Rename the 'count' column to 'brewery_quantity'
        golden_df = aggregated_df.withColumnRenamed('count', 'brewery_quantity')

        print(f"Writing gold layer data to SQLite database at: {jdbc_url}...")

        # Repartition the DataFrame to a single partition before writing
        golden_df_repartitioned = golden_df.repartition(1)

        # Write the 'golden_df' DataFrame to SQLite using JDBC
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
    # Get the Spark session
    spark = get_spark_session_from_args()
    transform_to_gold_layer(spark)
