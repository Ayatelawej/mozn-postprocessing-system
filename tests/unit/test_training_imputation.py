from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from postprocessing.training.imputation import (
    apply_imputation,
    compute_imputation_stats,
)


def _make_synthetic(n_stations: int = 2, n_hours: int = 48, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    base_time = pd.Timestamp("2025-07-01 00:00:00", tz="UTC")
    for s in range(n_stations):
        for h in range(n_hours):
            rows.append({
                "station_id": f"S{s:02d}",
                "issue_time_utc": base_time + pd.Timedelta(hours=h),
                "temp": 20.0 + 5.0 * np.sin(2 * np.pi * h / 24) + rng.normal(scale=0.5),
                "humidity": 60.0 + rng.normal(scale=5.0),
                "constant_feat": 1.0,
            })
    return pd.DataFrame(rows)


def test_compute_stats_returns_24_hours():
    df = _make_synthetic(n_hours=48)
    stats = compute_imputation_stats(df, ["temp", "humidity"])
    assert len(stats.hour_median) == 24
    assert set(stats.hour_median.columns) == {"temp", "humidity"}


def test_compute_stats_hour_median_matches_groupby():
    df = _make_synthetic(n_hours=48)
    stats = compute_imputation_stats(df, ["temp"])
    df["__h"] = df["issue_time_utc"].dt.hour
    expected = df.groupby("__h")["temp"].median()
    for h in expected.index:
        assert abs(stats.hour_median.loc[h, "temp"] - expected.loc[h]) < 1e-9


def test_compute_stats_missing_time_col_raises():
    df = pd.DataFrame({"x": [1.0, 2.0]})
    with pytest.raises(KeyError):
        compute_imputation_stats(df, ["x"])


def test_compute_stats_no_known_columns_raises():
    df = _make_synthetic()
    with pytest.raises(ValueError):
        compute_imputation_stats(df, ["nonexistent_col"])


def test_apply_imputation_fills_nan_from_hour_median():
    df = _make_synthetic(n_hours=72)
    stats = compute_imputation_stats(df, ["temp", "humidity"])
    df_with_nan = df.copy()
    df_with_nan.loc[0, "temp"] = np.nan
    df_with_nan.loc[10, "humidity"] = np.nan
    filled = apply_imputation(df_with_nan, stats)
    assert not filled["temp"].isna().any()
    assert not filled["humidity"].isna().any()
    h0 = int(df.loc[0, "issue_time_utc"].hour)
    expected_temp = stats.hour_median.loc[h0, "temp"]
    assert abs(filled.loc[0, "temp"] - expected_temp) < 1e-9


def test_apply_imputation_idempotent_on_complete_data():
    df = _make_synthetic(n_hours=48)
    stats = compute_imputation_stats(df, ["temp", "humidity"])
    filled = apply_imputation(df, stats)
    for col in ["temp", "humidity"]:
        assert (filled[col].to_numpy() == df[col].to_numpy()).all()


def test_apply_imputation_falls_back_to_global_when_hour_unknown():
    df = _make_synthetic(n_hours=24)
    stats = compute_imputation_stats(df, ["temp"])
    stats.hour_median.drop(index=5, inplace=True)
    df_with_nan = df.copy()
    target_row = df_with_nan.index[df_with_nan["issue_time_utc"].dt.hour == 5][0]
    df_with_nan.loc[target_row, "temp"] = np.nan
    filled = apply_imputation(df_with_nan, stats)
    assert not pd.isna(filled.loc[target_row, "temp"])
    assert abs(filled.loc[target_row, "temp"] - stats.global_median["temp"]) < 1e-9


def test_apply_imputation_ignores_unknown_columns():
    df = _make_synthetic(n_hours=24)
    stats = compute_imputation_stats(df, ["temp"])
    df["extra_col"] = np.nan
    filled = apply_imputation(df, stats)
    assert filled["extra_col"].isna().all()
