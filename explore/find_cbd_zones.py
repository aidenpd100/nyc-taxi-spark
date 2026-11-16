from pyspark.sql import functions as F
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from common import *

spark = get_spark("find-cbd")
y = read_service(spark, "yellow", YELLOW_COLS, months=["2025-03", "2025-04", "2025-05"])

by_zone = (y.groupBy("PULocationID")
             .agg(F.count("*").alias("trips"),
                  F.avg((F.col("cbd_congestion_fee") > 0).cast("int")).alias("share_charged"))
             .orderBy(F.desc("share_charged")))
by_zone.show(120, truncate=False)
zones_df = (by_zone.where((F.col("share_charged") >= 0.9) & (F.col("trips") >= 100))
                   .select("PULocationID").orderBy("PULocationID"))
zones = [r["PULocationID"] for r in zones_df.collect()]
print(sorted(zones))
print(len(zones))