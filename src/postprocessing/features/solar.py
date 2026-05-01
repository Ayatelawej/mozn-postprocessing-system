from __future__ import annotations

import numpy as np
import pandas as pd


def merge_daily_into_hourly(
    hourly: pd.DataFrame,
    daily: pd.DataFrame,
) -> pd.DataFrame:
    out = hourly.copy()
    out["_date"] = out["valid_time_utc"].dt.floor("D")

    daily_local = daily.copy()
    daily_local["_date"] = daily_local["date_utc"].dt.floor("D")
    daily_local = daily_local.drop(columns=["date_utc"])

    join_cols = ["station_id", "_date"]
    out = out.merge(daily_local, on=join_cols, how="left", suffixes=("", "_daily"))
    out = out.drop(columns=["_date"])
    return out


def add_solar_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    required = ["valid_time_utc", "base_sunrise_utc", "base_sunset_utc", "base_is_day"]
    missing = [c for c in required if c not in out.columns]
    if missing:
        raise KeyError(
            f"add_solar_features requires merged daily columns; missing: {missing}"
        )

    seconds_from_sunrise = (
        out["valid_time_utc"] - out["base_sunrise_utc"]
    ).dt.total_seconds()
    daylight_seconds = (
        out["base_sunset_utc"] - out["base_sunrise_utc"]
    ).dt.total_seconds()

    is_day = out["base_is_day"].astype(bool)

    minutes_from_sunrise = seconds_from_sunrise / 60.0
    out["minutes_from_sunrise"] = np.where(is_day, minutes_from_sunrise, 0.0)

    with np.errstate(divide="ignore", invalid="ignore"):
        progress = seconds_from_sunrise / daylight_seconds
    progress = np.clip(progress, 0.0, 1.0)
    out["solar_progress_0_1"] = np.where(is_day, progress, 0.0)

    out["solar_centered"] = np.where(
        is_day,
        np.sin(np.pi * out["solar_progress_0_1"]),
        0.0,
    )

    return out
