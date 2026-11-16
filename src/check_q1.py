import sys
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[*]")
         .config("spark.driver.memory", "8g").getOrCreate())

out = sys.argv[1] if len(sys.argv) > 1 else "data/out/q1_1mo"
df = spark.read.parquet(out)

print("rows:", df.count())          # expect 48 (2 licenses x 24 hours)
df.groupBy("hvfhs_license_num").count().show()
df.agg(F.sum("trips").alias("total_trips")).show()
df.orderBy("hvfhs_license_num", "hour").show(48, truncate=False)

spark.stop()