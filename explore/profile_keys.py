import statistics
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .master("local[*]")
         .appName("tlc-profiles")
         .config("spark.sql.shuffle.partitions", "16")
         .getOrCreate())

YELLOW = "" # set file path
HVFHV  = ""   # set file path

SHUFFLE_PARTS = 16


def key_profile(df, cols):
    """Distribution of the key itself. cols is a list of column names."""
    kc = df.groupBy(*cols).count().orderBy(F.desc("count")).collect()
    counts = [r["count"] for r in kc]
    med = statistics.median(counts)
    top = kc[0]
    return {"key": "+".join(cols), "distinct_keys": len(counts),
            "hottest_key": tuple(top[c] for c in cols),
            "hottest_key_rows": top["count"],
            "median_key_rows": med,
            "key_skew_ratio": round(counts[0] / med, 2)}


def partition_profile(df, cols, n=SHUFFLE_PARTS):
    """Rows per shuffle partition if df were hash-partitioned on cols."""
    prof = (df.repartition(n, *[F.col(c) for c in cols])
              .withColumn("_pid", F.spark_partition_id())
              .groupBy("_pid").count().collect())
    counts = [r["count"] for r in prof]
    med = statistics.median(counts)
    return {"partitions_nonempty": len(counts),
            "max_rows": max(counts), "median_rows": med, "min_rows": min(counts),
            "partition_skew_ratio": round(max(counts) / med, 2)}


def profile(df, cols, total):
    fair = total / SHUFFLE_PARTS
    kp = key_profile(df, cols)
    pp = partition_profile(df, cols)
    kp["hottest_vs_fair"] = round(kp["hottest_key_rows"] / fair, 2)
    print(f"\nKEY        {kp}")
    print(f"PARTITIONS {pp}")
    print(f"fair share = {fair:,.0f}   hottest/fair = {kp['hottest_vs_fair']}")


# ---------- yellow ----------
# y = spark.read.parquet(YELLOW).select("PULocationID", "DOLocationID", "payment_type").cache()
# y_total = y.count()
# print(f"yellow rows: {y_total:,}")

# profile(y, ["PULocationID"], y_total)                    # pickup zone
# profile(y, ["PULocationID", "DOLocationID"], y_total)    # zone pair
# profile(y, ["payment_type"], y_total)                    # tiny key space, for contrast

# ---------- HVFHV ----------
h = spark.read.parquet(HVFHV).select("hvfhs_license_num", "PULocationID", "DOLocationID").cache()
h_total = h.count()
print(f"hvfhv rows: {h_total:,}")
profile(h, ["hvfhs_license_num"], h_total)
profile(h, ["PULocationID"], h_total)
profile(h, ["PULocationID", "DOLocationID"], h_total)

spark.stop()