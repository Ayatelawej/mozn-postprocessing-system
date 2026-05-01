from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from postprocessing.features.lags import (
    LAG_OFFSETS_HOURS,
    LAG_VARIABLES,
    ROLLING_WINDOWS_HOURS,
    add_lag_columns,
    add_lag_features,
    add_rolling_stats,
)


def make_two_station_frame(periods: int = 30) -> pd.DataFrame:
    timestamps = pd.date_range("2025-10-09 00:00", periods=periods, freq="h", tz="UTC")
    temps_a = list(range(periods))
    temps_b = [v * 2 for v in range(periods)]
    return pd.DataFrame(
        {
            "station_id": ["A"] * periods + ["B"] * periods,
            "valid_time_utc": list(timestamps) + list(timestamps),
            "temperature_c": temps_a + temps_b,
            "relative_humidity_pct": [50.0] * (2 * periods),
            "wind_speed_kmh": [10.0] * (2 * periods),
        }
    )


def test_lag_constants_match_design() -> None:
    assert LAG_OFFSETS_HOURS == (1, 2, 3, 6, 12, 24)
    assert ROLLING_WINDOWS_HOURS == (3, 6, 12, 24)
    assert "temperature_c" in LAG_VARIABLES
    assert "pressure_max_hpa" in LAG_VARIABLES


def test_lag_columns_creates_one_per_offset_per_variable() -> None:
    df = make_two_station_frame()
    out = add_lag_columns(df)
    for h in LAG_OFFSETS_HOURS:
        assert f"temperature_c_lag_{h}h" in out.columns
        assert f"relative_humidity_pct_lag_{h}h" in out.columns


def test_lag_columns_skips_missing_variables() -> None:
    df = make_two_station_frame()
    out = add_lag_columns(df)
    for h in LAG_OFFSETS_HOURS:
        assert f"dew_point_c_lag_{h}h" not in out.columns
        assert f"pressure_max_hpa_lag_{h}h" not in out.columns


def test_lag_first_row_per_station_is_nan() -> None:
    df = make_two_station_frame()
    out = add_lag_columns(df)
    a = out[out["station_id"] == "A"].reset_index(drop=True)
    b = out[out["station_id"] == "B"].reset_index(drop=True)
    assert pd.isna(a.loc[0, "temperature_c_lag_1h"])
    assert pd.isna(b.loc[0, "temperature_c_lag_1h"])


def test_lag_1h_equals_previous_row() -> None:
    df = make_two_station_frame()
    out = add_lag_columns(df)
    a = out[out["station_id"] == "A"].reset_index(drop=True)
    for i in range(1, 10):
        assert a.loc[i, "temperature_c_lag_1h"] == a.loc[i - 1, "temperature_c"]


def test_lag_no_cross_station_leakage() -> None:
    df = make_two_station_frame()
    out = add_lag_columns(df)
    b = out[out["station_id"] == "B"].reset_index(drop=True)
    assert pd.isna(b.loc[0, "temperature_c_lag_1h"])


def test_lag_24h_alignment() -> None:
    df = make_two_station_frame(periods=30)
    out = add_lag_columns(df)
    a = out[out["station_id"] == "A"].reset_index(drop=True)
    for i in range(24):
        assert pd.isna(a.loc[i, "temperature_c_lag_24h"])
    assert a.loc[24, "temperature_c_lag_24h"] == 0.0
    assert a.loc[25, "temperature_c_lag_24h"] == 1.0


def test_rolling_stats_creates_mean_and_std() -> None:
    df = make_two_station_frame()
    out = add_rolling_stats(df)
    for w in ROLLING_WINDOWS_HOURS:
        assert f"temperature_c_roll_mean_{w}h" in out.columns
        assert f"temperature_c_roll_std_{w}h" in out.columns


def test_rolling_mean_3h_third_row() -> None:
    df = make_two_station_frame()
    out = add_rolling_stats(df)
    a = out[out["station_id"] == "A"].reset_index(drop=True)
    expected = (0 + 1 + 2) / 3.0
    assert np.isclose(a.loc[2, "temperature_c_roll_mean_3h"], expected)


def test_rolling_mean_first_row_is_self() -> None:
    df = make_two_station_frame()
    out = add_rolling_stats(df)
    a = out[out["station_id"] == "A"].reset_index(drop=True)
    assert a.loc[0, "temperature_c_roll_mean_3h"] == 0.0


def test_rolling_std_constant_value_is_zero() -> None:
    df = make_two_station_frame()
    out = add_rolling_stats(df)
    a = out[out["station_id"] == "A"].reset_index(drop=True)
    for i in range(2, 10):
        assert np.isclose(a.loc[i, "relative_humidity_pct_roll_std_3h"], 0.0, atol=1e-10)


def test_rolling_no_cross_station_leakage() -> None:
    df = make_two_station_frame()
    out = add_rolling_stats(df)
    b = out[out["station_id"] == "B"].reset_index(drop=True)
    assert b.loc[0, "temperature_c_roll_mean_3h"] == 0.0


def test_lags_raises_when_keys_missing() -> None:
    df = pd.DataFrame({"temperature_c": [1.0, 2.0, 3.0]})
    with pytest.raises(KeyError):
        add_lag_columns(df)
    with pytest.raises(KeyError):
        add_rolling_stats(df)


def test_add_lag_features_full_pipeline() -> None:
    df = make_two_station_frame()
    out = add_lag_features(df)
    assert "temperature_c_lag_1h" in out.columns
    assert "temperature_c_lag_24h" in out.columns
    assert "temperature_c_roll_mean_3h" in out.columns
    assert "temperature_c_roll_std_24h" in out.columns
