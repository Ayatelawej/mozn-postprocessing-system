from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class ImputationStats:
    hour_median: pd.DataFrame
    global_median: pd.Series
    feature_columns: tuple[str, ...]


def compute_imputation_stats(
    df: pd.DataFrame,
    feature_columns: list[str] | tuple[str, ...],
    *,
    time_col: str = "issue_time_utc",
) -> ImputationStats:
    if time_col not in df.columns:
        raise KeyError(f"time_col '{time_col}' not in DataFrame")
    cols = tuple(feature_columns)
    present = [c for c in cols if c in df.columns]
    if not present:
        raise ValueError("No feature columns found in DataFrame")
    hours = pd.to_datetime(df[time_col], utc=True).dt.hour.to_numpy()
    working = df[present].copy()
    working["__hour"] = hours
    hour_med = working.groupby("__hour")[present].median()
    global_med = df[present].median()
    return ImputationStats(
        hour_median=hour_med,
        global_median=global_med,
        feature_columns=cols,
    )


def apply_imputation(
    df: pd.DataFrame,
    stats: ImputationStats,
    *,
    time_col: str = "issue_time_utc",
) -> pd.DataFrame:
    if time_col not in df.columns:
        raise KeyError(f"time_col '{time_col}' not in DataFrame")
    out = df.copy()
    hours = pd.to_datetime(out[time_col], utc=True).dt.hour.to_numpy()
    aligned = stats.hour_median.reindex(hours)
    aligned.index = out.index
    for col in stats.feature_columns:
        if col not in out.columns:
            continue
        nan_mask = out[col].isna()
        if not nan_mask.any():
            continue
        out.loc[nan_mask, col] = aligned.loc[nan_mask, col]
        still_nan = out[col].isna()
        if still_nan.any() and col in stats.global_median.index:
            out.loc[still_nan, col] = stats.global_median[col]
    return out
