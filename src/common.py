# session builder, paths, baseline harness
import json, os, statistics, time
from contextlib import redirect_stdout
from io import StringIO
from pyspark.sql import SparkSession

RAW = "data/raw"
OUT = "data/out"

def get_spark(name):
    return (SparkSession.builder
            .master("local[*]")
            .appName(name)
            .config("spark.driver.memory", "6g")
            .config("spark.sql.session.timeZone", "America/New_York")
            .getOrCreate())

def save_plan(df, path):
    buf = StringIO()
    with redirect_stdout(buf):
        df.explain("formatted")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, "w").write(buf.getvalue())

def run_baseline(name, build_and_write, plan_df_fn, hold_s=300, trials=1):
    """build_and_write() runs the whole pipeline
    and returns nothing. plan_df_fn() returns the DataFrame whose plan to save."""
    d = f"baseline/{name}"
    os.makedirs(d, exist_ok=True)
    save_plan(plan_df_fn(), f"{d}/plan.txt")
    times = []
    for i in range(trials):
        t0 = time.perf_counter()
        build_and_write()
        times.append(round(time.perf_counter() - t0, 2))
        print(f"[{name}] run {i+1}: {times[-1]} s")
    json.dump({"query": name, "trials_s": times,
               "median_s": statistics.median(times)},
              open(f"{d}/timing.json", "w"), indent=2)
    print(f"Spark UI open at http://localhost:4040 for {hold_s}s")
    time.sleep(hold_s)