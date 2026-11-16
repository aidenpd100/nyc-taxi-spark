import glob, statistics
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[*]")
         .config("spark.driver.memory", "16g")
         .config("spark.sql.shuffle.partitions", "16").getOrCreate())

df = spark.read.parquet(*sorted(glob.glob("data/raw/fhvhv/*.parquet"))) \
          .select("hvfhs_license_num")
total = df.count()
rows = df.groupBy("hvfhs_license_num").count().orderBy(F.desc("count")).collect()
fair = total / 16
print(f"total {total:,}  fair share {fair:,.0f}")
for r in rows:
    print(r["hvfhs_license_num"], f"{r['count']:,}", f"{r['count']/total:.1%}",
          f"hottest/fair {r['count']/fair:.2f}")