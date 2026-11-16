from datetime import date
from pathlib import Path

import meteostat as ms
import pandas as pd

# Central Park, Manhattan
POINT = ms.Point(40.7829, -73.9654, 30)
START = date(2024, 1, 1)
END = date(2025, 12, 31)
OUT = Path("data/weather/central_park_daily.parquet")

# Fetch nearby station observations and interpolate them to Central Park.
stations = ms.stations.nearby(POINT, limit=4)
weather = ms.interpolate(ms.daily(stations, START, END), POINT).fetch()
weather = weather.reset_index().rename(columns={"time": "date"})

# Meteostat versions may call the snow-depth column either snwd or snow.
snow_column = "snwd" if "snwd" in weather.columns else "snow"

daily = pd.DataFrame(
    {
        "date": pd.to_datetime(weather["date"]).dt.date,
        "prcp_mm": pd.to_numeric(weather.get("prcp"), errors="coerce"),
        "snow_depth_cm": pd.to_numeric(
            weather.get(snow_column), errors="coerce"
        ),
    }
)

# Ensure exactly one row for every requested calendar date. Missing observations stay null.
calendar = pd.DataFrame({"date": pd.date_range(START, END, freq="D").date})
daily = calendar.merge(daily, on="date", how="left")

OUT.parent.mkdir(parents=True, exist_ok=True)
daily.to_parquet(OUT, index=False)

print(f"Wrote {len(daily)} daily weather rows to {OUT}")
print(f"Date range: {daily['date'].min()} through {daily['date'].max()}")
print(f"Missing precipitation values: {daily['prcp_mm'].isna().sum()}")
print(f"Missing snow-depth values: {daily['snow_depth_cm'].isna().sum()}")