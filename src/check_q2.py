from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[*]").getOrCreate()

q2 = spark.read.parquet(
    "data/out/q2_feb_dec_yoy/cohort_year_summary"
)

q2.orderBy("year", "cohort").show(truncate=False)

q2.agg(
    F.sum("trip_count").alias("all_q2_eligible_trips")
).show()