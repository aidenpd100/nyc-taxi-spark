# nyc-taxi-spark

Spark pipeline over NYC TLC trip records (yellow taxi and high-volume for-hire) to study
company-level trip performance, Manhattan congestion pricing, and demand shifts.
Big Data Systems course project.

**Team:** Aiden Deffner (adeffne2), Seth Wyzykowski

## Questions

1. **Q1, company performance:** how do driver pay per minute, tip rate and trip volume
   differ by `hvfhs_license_num` and hour? (percent_rank window partitioned by license)
2. **Q2, congestion pricing:** how did trips into the congestion zone change after
   January 2025? (`cbd_congestion_fee` identifies charged trips)
3. **Q3, demand shifts:** which zone pairs gained or lost demand month over month,
   and does weather explain part of it? (weather join)

## Predicted bottleneck (on record before building)

Skew in any shuffle keyed on `hvfhs_license_num`. Measured on the full scope before
running Q1: HV0003 holds 355,115,344 of 483,060,132 HVFHV rows (73.5%, 11.76x the fair
share at 16 partitions); HV0005 holds 127,944,788 (26.5%, 4.24x). Only 2 of 16 shuffle
partitions can receive data, whatever the partition count. See `explore/license_profile.txt`.

## Data

- Source: NYC TLC Trip Record Data (public Parquet), downloaded 2026-11-15.
- Scope: yellow and HVFHV, 2024-01 to 2025-12 (24 months each).
- Rows: yellow 89,892,322; HVFHV 483,060,132; total 572,952,454 (per-file counts in `data_counts.txt`).
- Size: yellow 1.4 GB; HVFHV 10.9 GB; total 12.3 GB (decimal GB; `data_sizes.txt`),
  about 38% of 32 GB RAM.
- Schema drift: `cbd_congestion_fee` is absent from early files and present in 2025-12
  (first month with it: [to find in Q2]); `Airport_fee` (yellow) vs `airport_fee` (HVFHV)
  differ in case. `read_service` in `src/common.py` casts each file to a canonical
  schema and unions, so missing columns become null.
- Timestamps are `timestamp_ntz` (local New York time, no zone conversion).
- Weather: .
- Zone lookup: [source].

## Environment

| Item | Value |
|---|---|
| OS | Windows 11 |
| CPU / RAM | 6 cores / 32 GB |
| Python | 3.12 (venv) |
| Spark | PySpark 3.5.3, `local[*]` |
| Hadoop native | winutils 3.3.x |
| Java | 17.0.21 |
| `spark.driver.memory` | 16g |
| Other Spark settings | defaults (AQE on, 200 shuffle partitions) |

All baseline and optimized runs happen on this machine in this venv. Do not change
these settings between baseline and optimized runs.

## Repo layout

    src/download_tlc.py     download raw files
    src/common.py           session, schema normalization, baseline harness
    src/q1_company_perf.py  Q1 (accepts month arguments; no args = all 24 months)
    src/check_q1.py         sanity check on Q1 output
    src/count_rows.py       per-file row counts
    explore/                early profiling scripts and outputs (proposal evidence)
    baseline/<run>/         plan.txt, timing.json, ui_slowest_stage.png, sanity_check.txt
    data/                   gitignored

## Baselines

| Query | Scope | Time | Trials | Plan | UI screenshot | Notes |
|---|---|---|---:|---|---|---|
| Q1 | HVFHV, 24 months, 483,057,328 rows after filters | 713.71 s (11.9 min) | 1 | `baseline/q1_all_24mo/plan.txt` | `baseline/q1_all_24mo/ui_slowest_stage.png` | see below |
| Q2 | | | | | | |
| Q3 | | | | | | |



## Baselines (Seth's Computer)
| Query | Scope | Time | Trials | Plan | UI screenshot | Notes |
| Q1 |HVFHV, 24 months, 483,057,328 rows after filters | | | | | |
| Q2 |22 months,  80,892,744 rows | 81.29s|1 |`baseline/q2_feb_dec_yoy/plan.txt` |`baseline/q2_feb_dec_yoy/ui_slow_stage.png` | see below|
| Q3 |24 months,  89,892,263 rows|39.32s | 1|`baseline/q3_all_24mo/plan.txt` |`baseline/q3_all_24mo/ui_slow_task.png` |see below |



### Q1 baseline findings

- Slowest stage: stage 50, **2 tasks** (one per license).
- HV0003 task: 355,112,544 records read (5.8 GiB), 11 min, 37.1 GiB memory spill, 7.6 GiB disk spill.
- HV0005 task: 127,944,784 records (2.1 GiB), 3.9 min, 13.2 GiB memory spill, 2.6 GiB disk spill.
- The HV0003 task accounts for about 93% of the 713.71 s wall clock (660 s). Min/max task
  duration is 2.8x, matching the row ratio (355.1M / 127.9M = 2.78).
- Output sanity check: 48 rows (2 licenses x 24 hours); `total_trips` 483,057,328 vs
  483,060,132 raw rows (2,804 removed by the `trip_time > 0` and `driver_pay >= 0` filter).
- **Prediction check:** skew prediction confirmed, but the failure mode I expected was wrong:
  I predicted the sort might run out of memory, but Spark spilled to disk and finished.
- Limits: single trial; run produced by the earlier version of the script (no month
  argument), same query logic.
- Optimization target (not yet attempted): raising `spark.sql.shuffle.partitions` cannot
  help with 2 keys; the partitioning key has to change (for example `(license, hour)`
  or a salt).

### Q2 Baseline Findings
- Slowest stage- stage 128, 361 tasks
- Ended up being a lot faster than expected
- most of slowdown is likely from reading/scanning every file multiple time (when looking at other sparkUI jobs, each of the slowest tasks due to that)
- can produce speedup from scaning less, more intellegent window paritioning, and improving skew on the max duration tasks, and improving the amound of paralleization (if we can make it so that there is less, there will be less overhead for the small tasks)

### Q3 Baseline findings 
- Slowest stage- stage ?, 361 tasks
- once again, ended up being a lot faster than expected
- slowdown from windowing, scanning and MSJ
- speedup can come from allowing spark to optimize, using BHJ, and not scanning multiple times
## Division of work

- Aiden: ingestion, `common.py`, Q1
- Seth: weather and zone lookup, Q3
- Shared: Q2, baseline capture, README

## Log of changes to the baseline

None so far. 
