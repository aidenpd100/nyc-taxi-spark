import sys
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[*]")
         .config("spark.driver.memory", "8g").getOrCreate())

path = sys.argv[1]
df = spark.read.parquet(path)
print("rows:", df.count())
df.printSchema()

if "trips" in df.columns:
    df.agg(F.sum("trips").alias("total_trips")).show()

order = [c for c in ("service", "month", "into_cbd", "hvfhs_license_num", "hour") if c in df.columns]
df.orderBy(*order).show(200, truncate=False)
spark.stop()