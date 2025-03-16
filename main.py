import libs.install_dependencies as install_dependencies
import data_architecture.codes.bronze_layer as bronze_layer 
import data_architecture.codes.silver_layer as silver_layer
import data_architecture.codes.golden_layer as golden_layer
from libs.spark_session_manager import SparkSessionManager

if __name__ == "__main__":
    # Fetch the data from the bronze layer
    # bronze_layer.fetch_brewery_data()

    jdbc_driver_path = "libs/sqlite-jdbc-3.36.0.3.jar"

    # Create a Spark session
    spark_manager = SparkSessionManager(driver_path=jdbc_driver_path)
    spark = spark_manager.get_spark_session()
    
    try:
        # Run the silver layer transformation
        silver_layer.transform_to_silver_layer(spark)
        
        # Run the golden layer transformation
        golden_layer.transform_to_gold_layer(spark)

        # Save golden layer data into a SQLite Database
        jdbc_url = "jdbc:sqlite:data_architecture/data_sources/brewery_data.db"  # Path to your SQLite database file
        table_name = "golden_layer"  # Name of the table where the data was saved

        # Load data from the SQLite table into a Spark DataFrame
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .load()

        # Show the DataFrame to verify the data
        df.show(10)

    finally:
        # Stop the Spark session after processing is complete
        spark_manager.stop_spark_session()

