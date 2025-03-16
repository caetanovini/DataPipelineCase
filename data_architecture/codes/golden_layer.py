def transform_to_gold_layer(spark):
    # Read the silver layer Parquet file
    silver_df = spark.read.parquet('data_architecture/data_sources/silver_layer.parquet')

    # Perform aggregation (group by 'state' and 'brewery_type')
    aggregated_df = silver_df.groupBy("state", "brewery_type").count()

    # Rename the 'count' column to 'brewery_quantity'
    golden_df = aggregated_df.withColumnRenamed('count', 'brewery_quantity')


    # Define SQLite connection properties
    jdbc_url = "jdbc:sqlite:data_architecture/data_sources/brewery_data.db"  # Path to your SQLite database file
    table_name = "golden_layer"  # Name of the table where the data will be saved
    
    try:
        print(f"Repartitioning and writing data to {table_name}...")

        # Repartition the DataFrame to a single partition before writing
        golden_df_repartitioned = golden_df.repartition(1)
        
        # Write the golden DataFrame to the SQLite database using JDBC
        golden_df_repartitioned.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .mode("overwrite") \
            .save()
        
        print(f"Data successfully written to SQLite table: {table_name}")
    except Exception as e:
        print(f"Error writing to SQLite: {e}")
