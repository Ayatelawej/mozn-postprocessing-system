from __future__ import annotations

import pandas as pd

CORE_RESIDUAL_PAIRS: tuple[tuple[str, str, str], ...] = (
    ("temperature_c", "base_temperature_c", "temperature_residual_c"),
    ("relative_humidity_pct", "base_relative_humidity_pct", "relative_humidity_residual_pct"),
    ("dew_point_c", "base_dew_point_c", "dew_point_residual_c"),
    ("wind_speed_kmh", "base_wind_speed_kmh", "wind_speed_residual_kmh"),
    ("wind_gust_kmh", "base_wind_gust_kmh", "wind_gust_residual_kmh"),
)


def add_core_residuals(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for station_col, base_col, residual_col in CORE_RESIDUAL_PAIRS:
        if station_col in out.columns and base_col in out.columns:
            out[residual_col] = out[station_col] - out[base_col]
    return out
