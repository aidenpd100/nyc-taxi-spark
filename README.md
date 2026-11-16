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
- Schema drift: `cbd_congestion_fee` is absent through 2024-12 and present from 2025-01
  (January 2025: 2.25M yellow and 6.3M HVFHV trips charged). Yellow has negative values
  (min -0.75, likely reversals), so `> 0` identifies charged trips. See `explore/cbd_start.txt`.
  `Airport_fee` (yellow) vs `airport_fee` (HVFHV) differ in case. `read_service` in
  `src/common.py` casts each file to a canonical schema and unions, so missing columns become null.
- Data quality: yellow contains a few dozen trips with pickup dates in 2002 to 2009 and
  2023-12; Q2 filters to the study window (2024-01-01 to 2025-12-31).
- Timestamps are `timestamp_ntz` (local New York time, no zone conversion).
- Zone lookup: `taxi_zone_lookup.csv` from the TLC trip record page.
- Weather: [source, station/grid point, date range, timezone handling] (Seth).

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

    src/download_tlc.py      download raw files
    src/common.py            session, schema normalization, baseline harness
    src/q1_company_perf.py   Q1 (accepts month arguments; no args = all 24 months)
    src/q2_congestion.py     Q2 (accepts month arguments; no args = all 24 months)
    src/cbd_zones.py         38 congestion-zone IDs (derived, see Q2 findings)
    src/check_q1.py          sanity check on Q1 output
    src/check_out.py         generic sanity check on any query output
    src/count_rows.py        per-file row counts
    explore/                 profiling scripts and outputs (proposal evidence):
                             license_profile.txt, find_cbd_zones.py, cbd_zone_profile.txt,
                             find_cbd_start.py, cbd_start.txt
    baseline/<run>/          plan.txt, timing.json, ui_slowest_stage.png, sanity_check.txt
    data/                    gitignored

## Baselines

| Query | Scope | Time | Trials | Plan | UI screenshot | Notes |
|---|---|---|---:|---|---|---|
| Q1 | HVFHV, 24 months, 483,057,328 rows after filters | 713.71 s (11.9 min) | 1 | `baseline/q1_all_24mo/plan.txt` | `baseline/q1_all_24mo/ui_slowest_stage.png` | see Q1 findings |
| Q2 (v1) | yellow + HVFHV, 24 months, 569,269,572 rows after filters | 36.47 s median (40.39, 36.11, 36.47); 42.29 s first cold run | 3 (+1 cold) | `baseline/q2_all/plan.txt` | `baseline/q2_all/ui_slowest_stage.png` | see Q2 findings |
| Q3 | | | | | | |

Timing convention: headline is the median of same-process trials (trials after the first
share a warm JVM and file cache); a cold first run is noted where available. Use the same
convention for optimized runs.

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
  argument), same query logic. Repeat with `trials=3` if time allows (about 36 min).
- Optimization target (not yet attempted): raising `spark.sql.shuffle.partitions` cannot
  help with 2 keys; the partitioning key has to change (for example `(license, hour)`
  or a salt).

### Q2 baseline findings

- **Zone definition:** 38 Manhattan zones where at least 90% of yellow pickups were charged
  `cbd_congestion_fee` (min 100 trips, Mar to May 2025); the next-highest zone is 0.61, so
  there is a clear gap. All 38 are south of 60th St per the zone lookup.
  `into_cbd` = drop-off inside the zone and pickup outside it.
- Slowest stage: stage 96, the scan, union and partial aggregation. 576 tasks, median 0.8 s,
  max 2 s, 572,952,454 input records, 88.5 KiB shuffle write.
- **Prediction check:** the skew bottleneck did not appear in Q2. Group keys
  (service, month, flag) are many, and partial aggregation shrinks the shuffle to almost
  nothing, so all cores stayed busy.
- Output sanity check: 96 rows (2 services x 24 months x 2 flags); 569,269,572 trips after
  filters (about 3.68M fewer than raw, 0.64%).
- **v0 vs v1:** v0 (39.78 s, `baseline/q2_all_v0/`) had no date window (it produced stray
  2002 to 2009 months) and averaged per-trip speeds, which inflated yellow mph. v1 adds the
  date window, an mph < 80 cap and speed as total miles / total hours, and is the baseline.
- Analysis plan: "before" = 2024-01 to 2024-12, "after" = 2025-02 onward (January 2025 is
  the transition month); compare the same calendar months year over year because of
  seasonality. Findings are "associated with", not causal.
- Limits: the screenshot is from the first cold run; `trials=3` was added to the script
  afterward (same query logic).

## Division of work

- Aiden: ingestion, `common.py`, Q1, Q2
- Seth: Q2, weather, Q3
- Shared: baseline capture, README

## Log of changes to the baseline

- 2026-11-15: Q2 v0 replaced by v1 (date window, ratio-of-sums mph, mph cap). Old run kept
  in `baseline/q2_all_v0/` (39.78 s).
- 2026-11-15: Q2 harness changed to `trials=3` after the first cold run (42.29 s).