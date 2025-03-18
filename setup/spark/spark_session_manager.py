from pyspark.sql import SparkSession

class SparkSessionManager:
    def __init__(self, app_name="BreweryDataPipeline", driver_path=None):
        self.app_name = app_name
        self.spark = None
        self.driver_path = driver_path

    def get_spark_session(self):
        """Return the SparkSession instance, creating it if necessary."""
        if self.spark is None:
            builder = SparkSession.builder.appName(self.app_name)

            # Add the JDBC driver for SQLite
            if self.driver_path:
                builder = builder.config("spark.jars", self.driver_path)  # Specify the path to the .jar file
                builder = builder.config("spark.driver.extraClassPath", self.driver_path)  # Extra classpath for the driver

            self.spark = builder.getOrCreate()

        return self.spark

    def stop_spark_session(self):
        """Stop the SparkSession and release resources."""
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
