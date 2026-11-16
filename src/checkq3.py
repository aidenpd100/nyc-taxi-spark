from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[*]").getOrCreate()

q3 = spark.read.parquet(
    "data/out/q3_all_24mo/od_month_weather_changes"
)

q3.agg(
    F.sum("trip_count").alias("all_q3_eligible_trips"),
    F.count("*").alias("od_pair_month_rows"),
    F.countDistinct("PULocationID", "DOLocationID").alias("od_pairs"),
).show()