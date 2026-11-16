from pyspark.sql import SparkSession
spark = (SparkSession.builder.master("local[*]")
         .config("spark.driver.memory", "8g").getOrCreate())
for f in ["data/raw/fhvhv/fhvhv_tripdata_2024-01.parquet",
          "data/raw/fhvhv/fhvhv_tripdata_2025-12.parquet",
          "data/raw/yellow/yellow_tripdata_2024-01.parquet",
          "data/raw/yellow/yellow_tripdata_2025-12.parquet"]:
    print(f)
    spark.read.parquet(f).printSchema()