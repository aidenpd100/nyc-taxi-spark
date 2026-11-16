import sys
from pyspark.sql import Window, functions as F

from common import OUT, get_spark

# Default checks the full run:
#   python src/check_q3.py
#
# For a smaller-month run, pass its output tag:
#   python src/check_q3.py q3_3mo

TAG = sys.argv[1] if len(sys.argv) > 1 else "q3_all"
RESULT_PATH = f"{OUT}/{TAG}"
WEATHER_PATH = "data/weather/central_park_daily.parquet"

spark = get_spark(f"check_{TAG}")

results = spark.read.parquet(RESULT_PATH)
weather = spark.read.parquet(WEATHER_PATH)

print("\n=== Q3 RESULT SCHEMA ===")
results.printSchema()

print("\n=== TOTAL ELIGIBLE TRIPS ===")
results.agg(
    F.sum("trips").alias("eligible_trip_count"),
    F.count("*").alias("service_month_od_rows"),
    F.countDistinct(
        "service",
        "PULocationID",
        "DOLocationID",
    ).alias("distinct_service_od_flows"),
).show(truncate=False)

print("\n=== TRIPS BY SERVICE AND MONTH ===")
results.groupBy("service", "month").agg(
    F.sum("trips").alias("trips"),
    F.count("*").alias("od_flows"),
).orderBy("service", "month").show(100, truncate=False)

print("\n=== WEATHER FILE CHECK ===")
weather.select(
    F.count("*").alias("weather_rows"),
    F.countDistinct("date").alias("distinct_weather_dates"),
    F.min("date").alias("first_weather_date"),
    F.max("date").alias("last_weather_date"),
    F.sum(
        F.when(F.col("prcp_mm").isNull(), 1).otherwise(0)
    ).alias("null_precipitation_rows"),
    F.sum(
        F.when(F.col("snow_depth_cm").isNull(), 1).otherwise(0)
    ).alias("null_snow_depth_rows"),
).show(truncate=False)

print("\n=== MONTHLY WEATHER USED BY Q3 ===")
results.select(
    "month",
    "monthly_precip_mm",
    "monthly_max_snow_depth_cm",
    "precip_change_mm",
    "snow_depth_change_cm",
).distinct().orderBy("month").show(30, truncate=False)

print("\n=== DUPLICATE SERVICE/MONTH/OD CHECK ===")
duplicates = (
    results.groupBy(
        "service",
        "month",
        "PULocationID",
        "DOLocationID",
    )
    .count()
    .where(F.col("count") > 1)
)

print("Duplicate key rows:", duplicates.count())
duplicates.show(20, truncate=False)

print("\n=== PREVIOUS-MONTH CHECK ===")
flow_window = Window.partitionBy(
    "service",
    "PULocationID",
    "DOLocationID",
).orderBy("month")

lag_check = results.withColumn(
    "flow_row_number",
    F.row_number().over(flow_window),
)

lag_check.agg(
    F.sum(
        F.when(F.col("flow_row_number") == 1, 1).otherwise(0)
    ).alias("first_observed_rows"),
    F.sum(
        F.when(
            (F.col("flow_row_number") > 1)
            & F.col("previous_month_trips").isNull(),
            1,
        ).otherwise(0),
    ).alias("unexpected_null_previous_trip_counts"),
).show(truncate=False)

print("\n=== TOP 20 GAINS ===")
results.where(F.col("gain_rank") <= 3).select(
    "service",
    "month",
    "gain_rank",
    "PULocationID",
    "DOLocationID",
    "previous_month_trips",
    "trips",
    "trip_change",
    "trip_pct_change",
    "monthly_precip_mm",
    "precip_change_mm",
    "monthly_max_snow_depth_cm",
    "snow_depth_change_cm",
).orderBy(
    "service",
    "month",
    "gain_rank",
).show(20, truncate=False)

print("\n=== TOP 20 LOSSES ===")
results.where(F.col("loss_rank") <= 3).select(
    "service",
    "month",
    "loss_rank",
    "PULocationID",
    "DOLocationID",
    "previous_month_trips",
    "trips",
    "trip_change",
    "trip_pct_change",
    "monthly_precip_mm",
    "precip_change_mm",
    "monthly_max_snow_depth_cm",
    "snow_depth_change_cm",
).orderBy(
    "month",
    "service",
    "loss_rank",
).show(200, truncate=False)


q3 = spark.read.parquet("data/out/q3_all")

city_month = (
    q3.groupBy("service", "month")
    .agg(
        F.sum("trips").alias("total_trips"),
        F.first(
            "monthly_precip_mm",
            ignorenulls=True,
        ).alias("monthly_precip_mm"),
        F.first(
            "monthly_max_snow_depth_cm",
            ignorenulls=True,
        ).alias("monthly_max_snow_depth_cm"),
        F.first(
            "precip_change_mm",
            ignorenulls=True,
        ).alias("precip_change_mm"),
        F.first(
            "snow_depth_change_cm",
            ignorenulls=True,
        ).alias("snow_depth_change_cm"),
    )
)

month_window = Window.partitionBy("service").orderBy("month")

city_month = (
    city_month.withColumn(
        "previous_month_trips",
        F.lag("total_trips").over(month_window),
    )
    .withColumn(
        "trip_change",
        F.col("total_trips") - F.col("previous_month_trips"),
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

city_month.orderBy("service", "month").show(100, truncate=False)

spark.stop()