from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from postprocessing.features.time import (
    add_hour_cyclical,
    add_is_day_flag,
    add_time_features,
)


def make_24h_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "valid_time_utc": pd.date_range(
                "2025-10-09 00:00",
                periods=24,
                freq="h",
                tz="UTC",
            ),
            "base_is_day": [0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0],
        }
    )


def test_hour_cyclical_adds_two_columns() -> None:
    df = make_24h_frame()
    out = add_hour_cyclical(df)
    assert "hour_sin" in out.columns
    assert "hour_cos" in out.columns
    assert len(out) == 24


def test_hour_cyclical_does_not_mutate_input() -> None:
    df = make_24h_frame()
    before_cols = list(df.columns)
    add_hour_cyclical(df)
    assert list(df.columns) == before_cols


def test_hour_cyclical_zero_hour_is_sin0_cos1() -> None:
    df = make_24h_frame()
    out = add_hour_cyclical(df)
    assert np.isclose(out.iloc[0]["hour_sin"], 0.0)
    assert np.isclose(out.iloc[0]["hour_cos"], 1.0)


def test_hour_cyclical_six_hour_is_sin1_cos0() -> None:
    df = make_24h_frame()
    out = add_hour_cyclical(df)
    assert np.isclose(out.iloc[6]["hour_sin"], 1.0)
    assert np.isclose(out.iloc[6]["hour_cos"], 0.0, atol=1e-10)


def test_hour_cyclical_neighbouring_hours_are_close() -> None:
    df = make_24h_frame()
    out = add_hour_cyclical(df)
    row_23 = out.iloc[23]
    row_0 = out.iloc[0]
    distance = np.sqrt(
        (row_23["hour_sin"] - row_0["hour_sin"]) ** 2
        + (row_23["hour_cos"] - row_0["hour_cos"]) ** 2
    )
    assert distance < 0.3


def test_hour_cyclical_opposing_hours_are_far() -> None:
    df = make_24h_frame()
    out = add_hour_cyclical(df)
    row_0 = out.iloc[0]
    row_12 = out.iloc[12]
    distance = np.sqrt(
        (row_0["hour_sin"] - row_12["hour_sin"]) ** 2
        + (row_0["hour_cos"] - row_12["hour_cos"]) ** 2
    )
    assert np.isclose(distance, 2.0)


def test_is_day_flag_basic() -> None:
    df = make_24h_frame()
    out = add_is_day_flag(df)
    assert "is_day_flag" in out.columns
    assert out["is_day_flag"].dtype == np.int8
    assert out.iloc[0]["is_day_flag"] == 0
    assert out.iloc[12]["is_day_flag"] == 1


def test_is_day_flag_raises_when_base_missing() -> None:
    df = make_24h_frame().drop(columns=["base_is_day"])
    with pytest.raises(KeyError):
        add_is_day_flag(df)


def test_add_time_features_full_pipeline() -> None:
    df = make_24h_frame()
    out = add_time_features(df)
    assert "hour_sin" in out.columns
    assert "hour_cos" in out.columns
    assert "is_day_flag" in out.columns


def test_add_time_features_without_base_is_day_skips_flag() -> None:
    df = make_24h_frame().drop(columns=["base_is_day"])
    out = add_time_features(df)
    assert "hour_sin" in out.columns
    assert "hour_cos" in out.columns
    assert "is_day_flag" not in out.columns
