from pyspark.sql import functions as F, Window
from common import *

spark = get_spark("q1-baseline")

def build():
    df = read_service(spark, "fhvhv", HVFHV_COLS)
    df = (df.where((F.col("trip_time") > 0) & (F.col("driver_pay") >= 0))
            .withColumn("pay_per_min", F.col("driver_pay") / (F.col("trip_time") / 60.0))
            .withColumn("hour", F.hour("pickup_datetime")))
    w = Window.partitionBy("hvfhs_license_num").orderBy("pay_per_min")
    ranked = df.withColumn("pay_pctile", F.percent_rank().over(w))
    return (ranked.groupBy("hvfhs_license_num", "hour")
                  .agg(F.count("*").alias("trips"),
                       F.avg("pay_pctile").alias("avg_pay_pctile"),
                       F.avg("pay_per_min").alias("avg_pay_per_min"),
                       F.avg(F.col("tips") / F.col("base_passenger_fare")).alias("avg_tip_rate")))

def run():
    build().write.mode("overwrite").parquet(f"{OUT}/q1")

run_baseline("q1", run, build)