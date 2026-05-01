from __future__ import annotations

import numpy as np
import pandas as pd


def add_hour_cyclical(df: pd.DataFrame, hour_col: str = "valid_time_utc") -> pd.DataFrame:
    out = df.copy()
    hours = out[hour_col].dt.hour
    out["hour_sin"] = np.sin(2 * np.pi * hours / 24)
    out["hour_cos"] = np.cos(2 * np.pi * hours / 24)
    return out


def add_is_day_flag(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "base_is_day" not in out.columns:
        raise KeyError(
            "add_is_day_flag requires 'base_is_day' column from Open-Meteo baseline."
        )
    out["is_day_flag"] = out["base_is_day"].astype("int8")
    return out


def add_time_features(df: pd.DataFrame, hour_col: str = "valid_time_utc") -> pd.DataFrame:
    out = add_hour_cyclical(df, hour_col=hour_col)
    if "base_is_day" in out.columns:
        out = add_is_day_flag(out)
    return out
