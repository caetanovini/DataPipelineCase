from pyspark.sql.functions import col

def transform_to_silver_layer(spark):
    # Read the raw JSON data using the provided Spark session
    df = spark.read.json("data_architecture/data_sources/bronze_layer.json")
    
    # Transform data and partition by brewery location
    df_transformed = df\
        .select('id', 'name', 'country', 'state', 'city', col("address_2").alias("village"),
                'postal_code', 'street', 'latitude', 'longitude', 'phone', 'brewery_type', 'website_url')

    # Write the data in Parquet format, partitioned by city and state
    df_transformed\
        .write\
        .mode("overwrite")\
        .partitionBy("state")\
        .parquet("data_architecture/data_sources/silver_layer.parquet")

if __name__ == "__main__":
    # Run your transformation function
    transform_to_silver_layer(spark)
    

