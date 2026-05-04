from __future__ import annotations

import pandas as pd


def add_pressure_avg(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "pressure_max_hpa" in out.columns and "pressure_min_hpa" in out.columns:
        out["pressure_avg_hpa"] = (out["pressure_max_hpa"] + out["pressure_min_hpa"]) / 2.0
    return out


def add_pressure_residuals(df: pd.DataFrame) -> pd.DataFrame:
    out = add_pressure_avg(df)
    baseline = "base_surface_pressure_hpa"
    if baseline not in out.columns:
        return out

    for station_col, residual_col in (
        ("pressure_max_hpa", "pressure_residual_max_hpa"),
        ("pressure_min_hpa", "pressure_residual_min_hpa"),
        ("pressure_avg_hpa", "pressure_residual_avg_hpa"),
    ):
        if station_col in out.columns:
            out[residual_col] = out[station_col] - out[baseline]
    return out
