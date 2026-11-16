import glob
from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local[*]")
         .config("spark.driver.memory", "8g")
         .getOrCreate())

for svc in ["yellow", "fhvhv"]:
    files = sorted(glob.glob(f"data/raw/{svc}/*.parquet"))
    total = 0
    for f in files:
        n = spark.read.parquet(f).count()
        total += n
        print(f"{f}: {n:,}", flush=True)
    print(f"== {svc}: {len(files)} files, {total:,} rows ==", flush=True)

spark.stop()