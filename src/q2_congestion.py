import sys
from pyspark.sql import functions as F
from common import *
from cbd_zones import CBD_ZONES

MONTHS = sys.argv[1:] or None
TAG = "q2_" + (f"{len(MONTHS)}mo" if MONTHS else "all")
spark = get_spark(TAG)

def prep_yellow():
    d = read_service(spark, "yellow", YELLOW_COLS, months=MONTHS)
    return d.select(
        F.lit("yellow").alias("service"),
        F.col("tpep_pickup_datetime").alias("pickup"),
        F.col("tpep_dropoff_datetime").alias("dropoff"),
        "PULocationID", "DOLocationID",
        F.col("trip_distance").alias("miles"))

def prep_fhvhv():
    d = read_service(spark, "fhvhv", HVFHV_COLS, months=MONTHS)
    return d.select(
        F.lit("fhvhv").alias("service"),
        F.col("pickup_datetime").alias("pickup"),
        F.col("dropoff_datetime").alias("dropoff"),
        "PULocationID", "DOLocationID",
        F.col("trip_miles").alias("miles"))

def build():
    df = prep_yellow().unionByName(prep_fhvhv())
    df = (df.where((F.col("pickup") >= "2024-01-01") & (F.col("pickup") < "2026-01-01"))
            .withColumn("minutes",
                        (F.unix_timestamp("dropoff") - F.unix_timestamp("pickup")) / 60.0)
            .where((F.col("minutes") > 1) & (F.col("minutes") < 240) & (F.col("miles") > 0.1))
            .withColumn("mph", F.col("miles") / (F.col("minutes") / 60.0))
            .where(F.col("mph") < 80)
            .withColumn("month", F.date_trunc("month", "pickup"))
            .withColumn("into_cbd", F.col("DOLocationID").isin(CBD_ZONES)
                                    & ~F.col("PULocationID").isin(CBD_ZONES)))
    return (df.groupBy("service", "month", "into_cbd")
              .agg(F.count("*").alias("trips"),
                   F.avg("minutes").alias("avg_minutes"),
                   (F.sum("miles") / (F.sum("minutes") / 60.0)).alias("avg_mph")))

def run():
    build().write.mode("overwrite").parquet(f"{OUT}/{TAG}")

run_baseline(TAG, run, build, hold_s=0, trials=3)