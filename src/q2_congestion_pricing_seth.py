from pyspark.sql import Window, functions as F

from common import *

TAG = "q2_feb_dec_yoy"
OUTPUT = f"{OUT}/{TAG}"

spark = get_spark(TAG)

'''
read yellow data, validate data, group by location ID, figure out which ones are in teh congestion pricing
'''
def build_zone_fee_rates():
    """Infer treated destination zones from positive CBD fees in Feb-Dec 2025."""
    yellow = read_service(spark, "yellow", YELLOW_COLS)

    #make sure to get only valid data
    valid_2025 = (
        yellow.where(
            F.col("tpep_pickup_datetime").isNotNull()
            & F.col("tpep_dropoff_datetime").isNotNull()
            & F.col("trip_distance").isNotNull()
            & (F.col("trip_distance") > 0)
            & F.col("DOLocationID").isNotNull()
        )
        .withColumn(
            "duration_seconds",
            F.expr(
                "timestampdiff("
                "SECOND, tpep_pickup_datetime, tpep_dropoff_datetime)"
            ),
        )
        .where(F.col("duration_seconds") > 0)
        .withColumn("year", F.year("tpep_pickup_datetime"))
        .withColumn("month_number", F.month("tpep_pickup_datetime"))
        .where((F.col("year") == 2025) & (F.col("month_number") >= 2))
    )

    return (
        valid_2025.groupBy("DOLocationID")
        .agg(
            F.count("*").alias("candidate_trip_count"),
            F.sum(
                F.when(F.col("cbd_congestion_fee") > 0, 1).otherwise(0)
            ).alias("positive_fee_trip_count"),
            F.avg(
                F.when(F.col("cbd_congestion_fee") > 0, 1.0).otherwise(0.0)
            ).alias("positive_fee_rate"),
        )
        .withColumn(
            "is_treated_zone",
            F.col("positive_fee_rate") >= F.lit(0.50),
        )
        .orderBy("DOLocationID")
    )


def build_summary():
    yellow = read_service(spark, "yellow", YELLOW_COLS)
    #make sure to use only valid data
    valid = (
        yellow.where(
            F.col("tpep_pickup_datetime").isNotNull()
            & F.col("tpep_dropoff_datetime").isNotNull()
            & F.col("trip_distance").isNotNull()
            & (F.col("trip_distance") > 0)
            & F.col("DOLocationID").isNotNull()
        )
        .withColumn(
            "duration_seconds",
            F.expr(
                "timestampdiff("
                "SECOND, tpep_pickup_datetime, tpep_dropoff_datetime)"
            ),
        )
        .where(F.col("duration_seconds") > 0)
        .withColumn("year", F.year("tpep_pickup_datetime"))
        .withColumn("month_number", F.month("tpep_pickup_datetime"))
        .where(
            F.col("year").isin(2024, 2025)
            & (F.col("month_number") >= 2)
        )
        .withColumn(
            "duration_minutes",
            F.col("duration_seconds") / F.lit(60.0),
        )
        .withColumn(
            "speed_mph",
            F.col("trip_distance")
            / (F.col("duration_seconds") / F.lit(3600.0)),
        )
    )
    #finds fee zones
    treated_ids = (
        build_zone_fee_rates()
        .where(F.col("is_treated_zone"))
        .select("DOLocationID")
        .withColumn("treated_match", F.lit(1))
    )

    classified = (
        valid.join(treated_ids, "DOLocationID", "left") #LEFT JOIN 
        .withColumn(
            "cohort",
            F.when(F.col("treated_match").isNotNull(), "treated")
            .otherwise("control"),
        )
    )
    #aggregate
    return (
        classified.groupBy("year", "cohort")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("duration_minutes").alias("avg_duration_minutes"),
            F.avg("speed_mph").alias("avg_speed_mph"),
        )
        .orderBy("cohort", "year")
    )


def build_year_changes(summary):
    by_cohort = Window.partitionBy("cohort").orderBy("year")
    
    changed = (
        summary.withColumn("previous_year", F.lag("year").over(by_cohort))
        .withColumn(
            "previous_trip_count",
            F.lag("trip_count").over(by_cohort),
        )
        .withColumn(
            "previous_avg_duration_minutes",
            F.lag("avg_duration_minutes").over(by_cohort),
        )
        .withColumn(
            "previous_avg_speed_mph",
            F.lag("avg_speed_mph").over(by_cohort),
        )
    )

    def percent_change(current, previous):
        return F.when(
            F.col(previous).isNotNull() & (F.col(previous) != 0),
            (F.col(current) - F.col(previous))
            / F.col(previous)
            * F.lit(100.0),
        )

    return (
        changed.withColumn(
            "trip_count_pct_change",
            percent_change("trip_count", "previous_trip_count"),
        )
        .withColumn(
            "avg_duration_pct_change",
            percent_change(
                "avg_duration_minutes",
                "previous_avg_duration_minutes",
            ),
        )
        .withColumn(
            "avg_speed_pct_change",
            percent_change("avg_speed_mph", "previous_avg_speed_mph"),
        )
    )


def build_did_summary(summary):
    changes_2025 = build_year_changes(summary).where(F.col("year") == 2025)

    treated = (
        changes_2025.where(F.col("cohort") == "treated")
        .select(
            F.col("previous_trip_count").alias("treated_2024_trip_count"),
            F.col("trip_count").alias("treated_2025_trip_count"),
            F.col("previous_avg_duration_minutes").alias(
                "treated_2024_avg_duration_minutes"
            ),
            F.col("avg_duration_minutes").alias(
                "treated_2025_avg_duration_minutes"
            ),
            F.col("previous_avg_speed_mph").alias(
                "treated_2024_avg_speed_mph"
            ),
            F.col("avg_speed_mph").alias("treated_2025_avg_speed_mph"),
            F.col("trip_count_pct_change").alias(
                "treated_trip_count_pct_change"
            ),
            F.col("avg_duration_pct_change").alias(
                "treated_duration_pct_change"
            ),
            F.col("avg_speed_pct_change").alias(
                "treated_speed_pct_change"
            ),
        )
    )

    control = (
        changes_2025.where(F.col("cohort") == "control")
        .select(
            F.col("previous_trip_count").alias("control_2024_trip_count"),
            F.col("trip_count").alias("control_2025_trip_count"),
            F.col("previous_avg_duration_minutes").alias(
                "control_2024_avg_duration_minutes"
            ),
            F.col("avg_duration_minutes").alias(
                "control_2025_avg_duration_minutes"
            ),
            F.col("previous_avg_speed_mph").alias(
                "control_2024_avg_speed_mph"
            ),
            F.col("avg_speed_mph").alias("control_2025_avg_speed_mph"),
            F.col("trip_count_pct_change").alias(
                "control_trip_count_pct_change"
            ),
            F.col("avg_duration_pct_change").alias(
                "control_duration_pct_change"
            ),
            F.col("avg_speed_pct_change").alias(
                "control_speed_pct_change"
            ),
        )
    )

    return (
        treated.crossJoin(control)
        .withColumn(
            "did_trip_count_pct_change",
            F.col("treated_trip_count_pct_change")
            - F.col("control_trip_count_pct_change"),
        )
        .withColumn(
            "did_duration_pct_change",
            F.col("treated_duration_pct_change")
            - F.col("control_duration_pct_change"),
        )
        .withColumn(
            "did_speed_pct_change",
            F.col("treated_speed_pct_change")
            - F.col("control_speed_pct_change"),
        )
    )


def run():
    zone_fee_rates = build_zone_fee_rates()
    summary = build_summary()
    did_summary = build_did_summary(summary)

    zone_fee_rates.write.mode("overwrite").parquet(
        f"{OUTPUT}/zone_fee_rates"
    )
    summary.write.mode("overwrite").parquet(
        f"{OUTPUT}/cohort_year_summary"
    )
    did_summary.write.mode("overwrite").parquet(
        f"{OUTPUT}/did_summary"
    )


run_baseline(TAG, run, build_summary)