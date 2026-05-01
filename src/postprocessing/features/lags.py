from __future__ import annotations

import pandas as pd

LAG_OFFSETS_HOURS = (1, 2, 3, 6, 12, 24)
ROLLING_WINDOWS_HOURS = (3, 6, 12, 24)

LAG_VARIABLES = (
    "temperature_c",
    "relative_humidity_pct",
    "dew_point_c",
    "wind_speed_kmh",
    "pressure_max_hpa",
    "rain_total_mm",
)


def add_lag_columns(
    df: pd.DataFrame,
    variables: tuple[str, ...] = LAG_VARIABLES,
    lags: tuple[int, ...] = LAG_OFFSETS_HOURS,
) -> pd.DataFrame:
    out = df.copy()
    if "station_id" not in out.columns or "valid_time_utc" not in out.columns:
        raise KeyError(
            "add_lag_columns requires 'station_id' and 'valid_time_utc' columns."
        )
    out = out.sort_values(["station_id", "valid_time_utc"]).reset_index(drop=True)

    for var in variables:
        if var not in out.columns:
            continue
        for h in lags:
            out[f"{var}_lag_{h}h"] = out.groupby("station_id")[var].shift(h)
    return out


def add_rolling_stats(
    df: pd.DataFrame,
    variables: tuple[str, ...] = LAG_VARIABLES,
    windows: tuple[int, ...] = ROLLING_WINDOWS_HOURS,
) -> pd.DataFrame:
    out = df.copy()
    if "station_id" not in out.columns or "valid_time_utc" not in out.columns:
        raise KeyError(
            "add_rolling_stats requires 'station_id' and 'valid_time_utc' columns."
        )
    out = out.sort_values(["station_id", "valid_time_utc"]).reset_index(drop=True)

    for var in variables:
        if var not in out.columns:
            continue
        for w in windows:
            grouped = out.groupby("station_id")[var].rolling(window=w, min_periods=1)
            out[f"{var}_roll_mean_{w}h"] = grouped.mean().reset_index(level=0, drop=True)
            out[f"{var}_roll_std_{w}h"] = grouped.std().reset_index(level=0, drop=True)
    return out


def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    out = add_lag_columns(df)
    out = add_rolling_stats(out)
    return out
