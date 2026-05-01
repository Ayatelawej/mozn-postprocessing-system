from __future__ import annotations

import pandas as pd

RAIN_EVENT_THRESHOLD_MM_H = 0.1
ROLLING_WINDOWS_HOURS = (3, 6)


def add_rain_event_flag(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for source in ("base_rain_total_mm", "rain_total_mm"):
        if source in out.columns:
            flag_col = "base_rain_event" if source.startswith("base_") else "station_rain_event"
            out[flag_col] = (out[source] > RAIN_EVENT_THRESHOLD_MM_H).astype("int8")
    return out


def add_rain_rolling_sums(
    df: pd.DataFrame,
    windows: tuple[int, ...] = ROLLING_WINDOWS_HOURS,
) -> pd.DataFrame:
    out = df.copy()
    if "station_id" not in out.columns or "valid_time_utc" not in out.columns:
        raise KeyError(
            "add_rain_rolling_sums requires 'station_id' and 'valid_time_utc' columns."
        )

    out = out.sort_values(["station_id", "valid_time_utc"]).reset_index(drop=True)

    for source, prefix in (
        ("base_rain_total_mm", "base_rain"),
        ("rain_total_mm", "station_rain"),
    ):
        if source not in out.columns:
            continue
        for w in windows:
            col = f"{prefix}_rolling_{w}h_mm"
            out[col] = (
                out.groupby("station_id")[source]
                .rolling(window=w, min_periods=1)
                .sum()
                .reset_index(level=0, drop=True)
            )
    return out


def add_rain_features(df: pd.DataFrame) -> pd.DataFrame:
    out = add_rain_event_flag(df)
    out = add_rain_rolling_sums(out)
    return out
