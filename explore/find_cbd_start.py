import glob
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[*]")
         .config("spark.driver.memory", "8g").getOrCreate())

for svc in ["yellow", "fhvhv"]:
    print(f"== {svc} ==")
    for f in sorted(glob.glob(f"data/raw/{svc}/*2024-1*.parquet") +
                    glob.glob(f"data/raw/{svc}/*2025-*.parquet")):
        d = spark.read.parquet(f)
        month = f.split("_")[-1].replace(".parquet", "")
        if "cbd_congestion_fee" not in d.columns:
            print(month, "column absent")
            continue
        r = d.agg(F.count("*").alias("n"),
                  F.sum((F.col("cbd_congestion_fee") > 0).cast("int")).alias("charged"),
                  F.min("cbd_congestion_fee").alias("min_fee"),
                  F.max("cbd_congestion_fee").alias("max_fee")).first()
        print(month, f"rows={r['n']:,} charged={r['charged']} min={r['min_fee']} max={r['max_fee']}")

spark.stop()