from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from postprocessing.features.rain import (
    RAIN_EVENT_THRESHOLD_MM_H,
    add_rain_event_flag,
    add_rain_features,
    add_rain_rolling_sums,
)


def make_two_station_frame() -> pd.DataFrame:
    timestamps_a = pd.date_range("2025-10-09 00:00", periods=6, freq="h", tz="UTC")
    timestamps_b = pd.date_range("2025-10-09 00:00", periods=6, freq="h", tz="UTC")
    return pd.DataFrame(
        {
            "station_id": ["A"] * 6 + ["B"] * 6,
            "valid_time_utc": list(timestamps_a) + list(timestamps_b),
            "base_rain_total_mm": [0.0, 0.05, 0.1, 0.5, 1.5, 0.0,  0.0, 0.0, 0.0, 2.0, 0.5, 0.0],
            "rain_total_mm":      [0.0, 0.0, 0.2, 0.6, 1.2, 0.1,  0.0, 0.0, 0.0, 1.8, 0.3, 0.0],
        }
    )


def test_threshold_constant_value() -> None:
    assert RAIN_EVENT_THRESHOLD_MM_H == 0.1


def test_event_flag_strict_greater_than() -> None:
    df = pd.DataFrame({"base_rain_total_mm": [0.0, 0.1, 0.10001, 0.5]})
    out = add_rain_event_flag(df)
    assert out["base_rain_event"].tolist() == [0, 0, 1, 1]


def test_event_flag_dtype_is_int8() -> None:
    df = pd.DataFrame({"base_rain_total_mm": [0.0, 0.5]})
    out = add_rain_event_flag(df)
    assert out["base_rain_event"].dtype == np.int8


def test_event_flag_handles_both_sources() -> None:
    df = pd.DataFrame(
        {
            "base_rain_total_mm": [0.0, 0.5],
            "rain_total_mm": [0.5, 0.0],
        }
    )
    out = add_rain_event_flag(df)
    assert "base_rain_event" in out.columns
    assert "station_rain_event" in out.columns
    assert out["base_rain_event"].tolist() == [0, 1]
    assert out["station_rain_event"].tolist() == [1, 0]


def test_event_flag_handles_only_baseline() -> None:
    df = pd.DataFrame({"base_rain_total_mm": [0.0, 0.5]})
    out = add_rain_event_flag(df)
    assert "base_rain_event" in out.columns
    assert "station_rain_event" not in out.columns


def test_event_flag_no_rain_columns_no_op() -> None:
    df = pd.DataFrame({"other": [1, 2]})
    out = add_rain_event_flag(df)
    assert list(out.columns) == ["other"]


def test_rolling_sums_basic_shapes() -> None:
    df = make_two_station_frame()
    out = add_rain_rolling_sums(df)
    assert "base_rain_rolling_3h_mm" in out.columns
    assert "base_rain_rolling_6h_mm" in out.columns
    assert "station_rain_rolling_3h_mm" in out.columns
    assert "station_rain_rolling_6h_mm" in out.columns
    assert len(out) == 12


def test_rolling_sums_first_row_is_just_itself() -> None:
    df = make_two_station_frame()
    out = add_rain_rolling_sums(df)
    a_first = out[(out["station_id"] == "A")].iloc[0]
    assert np.isclose(a_first["base_rain_rolling_3h_mm"], 0.0)


def test_rolling_sums_third_row_includes_three_rows() -> None:
    df = make_two_station_frame()
    out = add_rain_rolling_sums(df)
    a_rows = out[out["station_id"] == "A"].reset_index(drop=True)
    expected = 0.0 + 0.05 + 0.1
    assert np.isclose(a_rows.loc[2, "base_rain_rolling_3h_mm"], expected)


def test_rolling_sums_per_station_no_leakage() -> None:
    df = make_two_station_frame()
    out = add_rain_rolling_sums(df)
    b_rows = out[out["station_id"] == "B"].reset_index(drop=True)
    assert np.isclose(b_rows.loc[0, "base_rain_rolling_3h_mm"], 0.0)
    assert np.isclose(b_rows.loc[2, "base_rain_rolling_3h_mm"], 0.0)


def test_rolling_sums_six_hour_window_full() -> None:
    df = make_two_station_frame()
    out = add_rain_rolling_sums(df)
    a_rows = out[out["station_id"] == "A"].reset_index(drop=True)
    expected = 0.0 + 0.05 + 0.1 + 0.5 + 1.5 + 0.0
    assert np.isclose(a_rows.loc[5, "base_rain_rolling_6h_mm"], expected)


def test_rolling_sums_raises_when_keys_missing() -> None:
    df = pd.DataFrame({"base_rain_total_mm": [0.0, 0.5]})
    with pytest.raises(KeyError):
        add_rain_rolling_sums(df)


def test_add_rain_features_full_pipeline() -> None:
    df = make_two_station_frame()
    out = add_rain_features(df)
    assert "base_rain_event" in out.columns
    assert "station_rain_event" in out.columns
    assert "base_rain_rolling_3h_mm" in out.columns
    assert "station_rain_rolling_6h_mm" in out.columns
