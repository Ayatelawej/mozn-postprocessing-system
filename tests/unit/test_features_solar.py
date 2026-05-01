from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from postprocessing.features.solar import (
    add_solar_features,
    merge_daily_into_hourly,
)


def make_hourly_two_days() -> pd.DataFrame:
    timestamps = pd.date_range(
        "2025-10-09 00:00",
        periods=48,
        freq="h",
        tz="UTC",
    )
    is_day = [int(6 <= ts.hour <= 18) for ts in timestamps]
    return pd.DataFrame(
        {
            "station_id": ["TEST"] * 48,
            "valid_time_utc": timestamps,
            "base_is_day": is_day,
        }
    )


def make_daily_two_days() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "station_id": ["TEST", "TEST"],
            "date_utc": pd.to_datetime(
                ["2025-10-09", "2025-10-10"], utc=True
            ),
            "base_sunrise_utc": pd.to_datetime(
                ["2025-10-09 06:00", "2025-10-10 06:02"], utc=True
            ),
            "base_sunset_utc": pd.to_datetime(
                ["2025-10-09 18:00", "2025-10-10 17:58"], utc=True
            ),
            "base_daylight_seconds": [43200, 43080],
        }
    )


def test_merge_daily_adds_columns() -> None:
    hourly = make_hourly_two_days()
    daily = make_daily_two_days()
    merged = merge_daily_into_hourly(hourly, daily)
    assert "base_sunrise_utc" in merged.columns
    assert "base_sunset_utc" in merged.columns
    assert "base_daylight_seconds" in merged.columns
    assert "date_utc" not in merged.columns
    assert "_date" not in merged.columns
    assert len(merged) == 48


def test_merge_daily_correct_day_alignment() -> None:
    hourly = make_hourly_two_days()
    daily = make_daily_two_days()
    merged = merge_daily_into_hourly(hourly, daily)
    row_oct9 = merged[merged["valid_time_utc"] == "2025-10-09 12:00+00:00"].iloc[0]
    row_oct10 = merged[merged["valid_time_utc"] == "2025-10-10 12:00+00:00"].iloc[0]
    assert row_oct9["base_sunrise_utc"] == pd.Timestamp("2025-10-09 06:00", tz="UTC")
    assert row_oct10["base_sunrise_utc"] == pd.Timestamp("2025-10-10 06:02", tz="UTC")


def test_merge_daily_does_not_mutate() -> None:
    hourly = make_hourly_two_days()
    daily = make_daily_two_days()
    before_hourly = list(hourly.columns)
    before_daily = list(daily.columns)
    merge_daily_into_hourly(hourly, daily)
    assert list(hourly.columns) == before_hourly
    assert list(daily.columns) == before_daily


def test_solar_features_creates_three_columns() -> None:
    hourly = make_hourly_two_days()
    daily = make_daily_two_days()
    merged = merge_daily_into_hourly(hourly, daily)
    out = add_solar_features(merged)
    assert "minutes_from_sunrise" in out.columns
    assert "solar_progress_0_1" in out.columns
    assert "solar_centered" in out.columns


def test_solar_features_at_sunrise() -> None:
    hourly = make_hourly_two_days()
    daily = make_daily_two_days()
    merged = merge_daily_into_hourly(hourly, daily)
    out = add_solar_features(merged)
    row_06 = out[out["valid_time_utc"] == "2025-10-09 06:00+00:00"].iloc[0]
    assert np.isclose(row_06["minutes_from_sunrise"], 0.0)
    assert np.isclose(row_06["solar_progress_0_1"], 0.0)
    assert np.isclose(row_06["solar_centered"], 0.0, atol=1e-10)


def test_solar_features_at_noon() -> None:
    hourly = make_hourly_two_days()
    daily = make_daily_two_days()
    merged = merge_daily_into_hourly(hourly, daily)
    out = add_solar_features(merged)
    row_12 = out[out["valid_time_utc"] == "2025-10-09 12:00+00:00"].iloc[0]
    assert np.isclose(row_12["minutes_from_sunrise"], 360.0)
    assert np.isclose(row_12["solar_progress_0_1"], 0.5)
    assert np.isclose(row_12["solar_centered"], 1.0)


def test_solar_features_zeroed_at_night() -> None:
    hourly = make_hourly_two_days()
    daily = make_daily_two_days()
    merged = merge_daily_into_hourly(hourly, daily)
    out = add_solar_features(merged)
    row_03 = out[out["valid_time_utc"] == "2025-10-09 03:00+00:00"].iloc[0]
    assert row_03["minutes_from_sunrise"] == 0.0
    assert row_03["solar_progress_0_1"] == 0.0
    assert row_03["solar_centered"] == 0.0


def test_solar_centered_peaks_at_progress_half() -> None:
    hourly = make_hourly_two_days()
    daily = make_daily_two_days()
    merged = merge_daily_into_hourly(hourly, daily)
    out = add_solar_features(merged)
    daytime = out[out["base_is_day"] == 1]
    idx_max = daytime["solar_centered"].idxmax()
    progress_at_peak = daytime.loc[idx_max, "solar_progress_0_1"]
    assert 0.4 < progress_at_peak < 0.6


def test_solar_features_progress_in_range() -> None:
    hourly = make_hourly_two_days()
    daily = make_daily_two_days()
    merged = merge_daily_into_hourly(hourly, daily)
    out = add_solar_features(merged)
    assert (out["solar_progress_0_1"] >= 0).all()
    assert (out["solar_progress_0_1"] <= 1).all()


def test_solar_features_raises_when_columns_missing() -> None:
    df = pd.DataFrame(
        {
            "valid_time_utc": pd.date_range("2025-10-09", periods=3, freq="h", tz="UTC"),
            "base_is_day": [0, 1, 1],
        }
    )
    with pytest.raises(KeyError):
        add_solar_features(df)
