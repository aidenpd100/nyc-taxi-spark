from pyspark.sql import Window, functions as F

from common import *

TAG = "q3_all_24mo"
OUTPUT = f"{OUT}/{TAG}"
WEATHER_PARQUET = "data/weather/central_park_daily.parquet"

spark = get_spark(TAG)

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", -1)


def build_weather():
    weather = (
        spark.read.parquet(WEATHER_PARQUET)
        .select(
            F.to_date("date").alias("pickup_date"),
            F.col("prcp_mm").cast("double").alias("prcp_mm"),
            F.col("snow_depth_cm").cast("double").alias(
                "snow_depth_cm"
            ),
        )
        .where(
            (F.col("pickup_date") >= F.lit("2024-01-01"))
            & (F.col("pickup_date") <= F.lit("2025-12-31"))
        )
    )

    return weather


def build_monthly_weather(weather):
    return (
        weather.withColumn(
            "month",
            F.date_trunc("month", F.col("pickup_date")).cast("date"),
        )
        .groupBy("month")
        .agg(
            F.sum("prcp_mm").alias("monthly_prcp_mm"),
            F.max("snow_depth_cm").alias("monthly_max_snow_depth_cm"),
        )
        .orderBy("month")
    )


def build_changes():
    weather = build_weather()

    yellow = read_service(spark, "yellow", YELLOW_COLS)

    valid_trips = (
        yellow.where(
            F.col("tpep_pickup_datetime").isNotNull()
            & F.col("PULocationID").isNotNull()
            & F.col("DOLocationID").isNotNull()
        )
        .withColumn(
            "pickup_date",
            F.to_date("tpep_pickup_datetime"),
        )
        .where(
            (F.col("pickup_date") >= F.lit("2024-01-01"))
            & (F.col("pickup_date") <= F.lit("2025-12-31"))
        )
    )

    daily_od_counts = (
        valid_trips.groupBy(
            "pickup_date",
            "PULocationID",
            "DOLocationID",
        )
        .agg(F.count("*").alias("daily_trip_count"))
    )

    #MSJ
    daily_with_weather = daily_od_counts.hint("merge").join(
        weather.hint("merge"),
        "pickup_date",
        "left",
    ).withColumn(
        "month",
        F.date_trunc("month", F.col("pickup_date")).cast("date"),
    )

    monthly_od_counts = (
        daily_with_weather.groupBy(
            "month",
            "PULocationID",
            "DOLocationID",
        )
        .agg(F.sum("daily_trip_count").alias("trip_count"))
    )

    # Full pair-by-month grid preserves complete demand losses as zero counts.
    all_pairs = daily_od_counts.select(
        "PULocationID",
        "DOLocationID",
    ).distinct()

    months = spark.range(24).select(
        F.add_months(
            F.to_date(F.lit("2024-01-01")),
            F.col("id").cast("int"),
        ).alias("month")
    )

    pair_month_grid = all_pairs.crossJoin(months)

    complete_counts = (
        pair_month_grid.hint("merge").join(
            monthly_od_counts.hint("merge"),
            ["month", "PULocationID", "DOLocationID"],
            "left",
    )
        .withColumn("trip_count", F.coalesce("trip_count", F.lit(0)))
    )

    monthly_weather = build_monthly_weather(weather)
    weather_window = Window.orderBy("month")

    monthly_weather_with_changes = (
        monthly_weather.withColumn(
            "previous_monthly_prcp_mm",
            F.lag("monthly_prcp_mm").over(weather_window),
        )
        .withColumn(
            "previous_monthly_max_snow_depth_cm",
            F.lag("monthly_max_snow_depth_cm").over(weather_window),
        )
        .withColumn(
            "monthly_prcp_mm_change",
            F.col("monthly_prcp_mm")
            - F.col("previous_monthly_prcp_mm"),
        )
        .withColumn(
            "monthly_max_snow_depth_cm_change",
            F.col("monthly_max_snow_depth_cm")
            - F.col("previous_monthly_max_snow_depth_cm"),
        )
    )

    with_weather = complete_counts.hint("merge").join(
        monthly_weather_with_changes.hint("merge"),
        "month",
        "left",
    )

    pair_window = Window.partitionBy(
        "PULocationID",
        "DOLocationID",
    ).orderBy("month")

    return (
        with_weather.withColumn(
            "previous_month_trip_count",
            F.lag("trip_count").over(pair_window),
        )
        .withColumn(
            "trip_count_change",
            F.col("trip_count") - F.col("previous_month_trip_count"),
        )
        .withColumn(
            "trip_count_pct_change",
            F.when(
                F.col("previous_month_trip_count") > 0,
                F.col("trip_count_change")
                / F.col("previous_month_trip_count")
                * F.lit(100.0),
            ),
        )
    )


def build_top_changes(changes):
    gain_window = Window.partitionBy("month").orderBy(
        F.col("trip_count_change").desc(),
        F.col("PULocationID"),
        F.col("DOLocationID"),
    )

    loss_window = Window.partitionBy("month").orderBy(
        F.col("trip_count_change").asc(),
        F.col("PULocationID"),
        F.col("DOLocationID"),
    )

    gains = (
        changes.where(F.col("trip_count_change") > 0)
        .withColumn("rank", F.row_number().over(gain_window))
        .where(F.col("rank") <= 20)
        .withColumn("direction", F.lit("gain"))
    )

    losses = (
        changes.where(F.col("trip_count_change") < 0)
        .withColumn("rank", F.row_number().over(loss_window))
        .where(F.col("rank") <= 20)
        .withColumn("direction", F.lit("loss"))
    )

    return gains.unionByName(losses)


def run():
    weather = build_weather()

    # Small validation output shown before the baseline run.
    print(
        "Weather rows:",
        weather.count(),
        "distinct dates:",
        weather.select("pickup_date").distinct().count(),
    )

    changes = build_changes()
    top_changes = build_top_changes(changes)

    changes.write.mode("overwrite").parquet(
        f"{OUTPUT}/od_month_weather_changes"
    )
    top_changes.write.mode("overwrite").parquet(
        f"{OUTPUT}/top_20_gains_losses"
    )


run_baseline(TAG, run, build_changes)