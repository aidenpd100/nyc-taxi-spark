import sys
from pyspark.sql import functions as F, Window

from common import *

MONTHS = sys.argv[1:] or None
TAG = "q3_" + (f"{len(MONTHS)}mo" if MONTHS else "all")
WEATHER = "data/weather/central_park_daily.parquet"

spark = get_spark(TAG)

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")

def prep_yellow():
    d = read_service(spark, "yellow", YELLOW_COLS, months=MONTHS)

    return d.select(
        F.lit("yellow").alias("service"),
        F.col("tpep_pickup_datetime").alias("pickup"),
        "PULocationID",
        "DOLocationID",
    )


def prep_fhvhv():
    d = read_service(spark, "fhvhv", HVFHV_COLS, months=MONTHS)

    return d.select(
        F.lit("fhvhv").alias("service"),
        F.col("pickup_datetime").alias("pickup"),
        "PULocationID",
        "DOLocationID",
    )


def prep_weather():
    daily = (
        spark.read.parquet(WEATHER)
        .select(
            F.to_date("date").alias("weather_date"),
            F.col("prcp_mm").cast("double").alias("prcp_mm"),
            F.col("snow_depth_cm").cast("double").alias(
                "snow_depth_cm"
            ),
        )
        .where(
            (F.col("weather_date") >= "2024-01-01")
            & (F.col("weather_date") < "2026-01-01")
        )
        .withColumn(
            "month",
            F.date_trunc("month", "weather_date"),
        )
    )

    monthly = (
        daily.groupBy("month")
        .agg(
            F.sum("prcp_mm").alias("monthly_precip_mm"),
            F.max("snow_depth_cm").alias(
                "monthly_max_snow_depth_cm"
            ),
        )
    )

    weather_window = Window.orderBy("month")

    return (
        monthly.withColumn(
            "previous_monthly_precip_mm",
            F.lag("monthly_precip_mm").over(weather_window),
        )
        .withColumn(
            "previous_monthly_max_snow_depth_cm",
            F.lag("monthly_max_snow_depth_cm").over(weather_window),
        )
        .withColumn(
            "precip_change_mm",
            F.col("monthly_precip_mm")
            - F.col("previous_monthly_precip_mm"),
        )
        .withColumn(
            "snow_depth_change_cm",
            F.col("monthly_max_snow_depth_cm")
            - F.col("previous_monthly_max_snow_depth_cm"),
        )
    )


def build():
    trips = prep_yellow().unionByName(prep_fhvhv())

    trips = (
        trips.where(
            F.col("pickup").isNotNull()
            & F.col("PULocationID").isNotNull()
            & F.col("DOLocationID").isNotNull()
        )
        .where(
            (F.col("pickup") >= "2024-01-01")
            & (F.col("pickup") < "2026-01-01")
        )
        .withColumn(
            "month",
            F.date_trunc("month", "pickup"),
        )
    )

    monthly_flows = (
        trips.groupBy(
            "service",
            "month",
            "PULocationID",
            "DOLocationID",
        )
        .agg(F.count("*").alias("trips"))
    )

    weather = prep_weather()

    with_weather = monthly_flows.hint("merge").join(
        weather.hint("merge"),
        "month",
        "left",
    )

    flow_window = (
        Window.partitionBy(
            "service",
            "PULocationID",
            "DOLocationID",
        )
        .orderBy("month")
    )

    with_changes = (
        with_weather.withColumn(
            "previous_month_trips",
            F.lag("trips").over(flow_window),
        )
        .withColumn(
            "trip_change",
            F.col("trips") - F.col("previous_month_trips"),
        )
        .withColumn(
            "trip_pct_change",
            F.when(
                F.col("previous_month_trips") > 0,
                F.col("trip_change")
                / F.col("previous_month_trips")
                * 100.0,
            ),
        )
    )

    gain_window = Window.partitionBy(
        "service",
        "month",
    ).orderBy(
        F.col("trip_change").desc_nulls_last(),
        "PULocationID",
        "DOLocationID",
    )

    loss_window = Window.partitionBy(
        "service",
        "month",
    ).orderBy(
        F.col("trip_change").asc_nulls_last(),
        "PULocationID",
        "DOLocationID",
    )

    return (
        with_changes.withColumn(
            "gain_rank",
            F.when(
                F.col("trip_change") > 0,
                F.row_number().over(gain_window),
            ),
        )
        .withColumn(
            "loss_rank",
            F.when(
                F.col("trip_change") < 0,
                F.row_number().over(loss_window),
            ),
        )
    )


def run():
    build().write.mode("overwrite").parquet(f"{OUT}/{TAG}")


run_baseline(TAG, run, build, hold_s=600, trials=1)