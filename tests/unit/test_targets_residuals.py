from __future__ import annotations

import numpy as np
import pandas as pd

from postprocessing.targets.residuals import (
    CORE_RESIDUAL_PAIRS,
    add_core_residuals,
)


def make_full_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "temperature_c": [20.0, 21.5, 22.0],
            "base_temperature_c": [21.0, 21.0, 21.0],
            "relative_humidity_pct": [60.0, 65.0, 70.0],
            "base_relative_humidity_pct": [55.0, 60.0, 60.0],
            "dew_point_c": [12.0, 13.5, 14.0],
            "base_dew_point_c": [11.0, 12.5, 13.0],
            "wind_speed_kmh": [10.0, 12.0, 15.0],
            "base_wind_speed_kmh": [9.0, 11.0, 13.0],
            "wind_gust_kmh": [18.0, 20.0, 25.0],
            "base_wind_gust_kmh": [16.0, 19.0, 22.0],
        }
    )


def test_pairs_constant_has_five_residuals() -> None:
    assert len(CORE_RESIDUAL_PAIRS) == 5
    residual_columns = [r[2] for r in CORE_RESIDUAL_PAIRS]
    assert "temperature_residual_c" in residual_columns
    assert "wind_gust_residual_kmh" in residual_columns


def test_residuals_arithmetic_correct() -> None:
    df = make_full_frame()
    out = add_core_residuals(df)
    assert out["temperature_residual_c"].tolist() == [-1.0, 0.5, 1.0]
    assert out["relative_humidity_residual_pct"].tolist() == [5.0, 5.0, 10.0]
    assert out["dew_point_residual_c"].tolist() == [1.0, 1.0, 1.0]
    assert out["wind_speed_residual_kmh"].tolist() == [1.0, 1.0, 2.0]
    assert out["wind_gust_residual_kmh"].tolist() == [2.0, 1.0, 3.0]


def test_residuals_propagate_nan() -> None:
    df = pd.DataFrame(
        {
            "temperature_c": [20.0, np.nan, 22.0],
            "base_temperature_c": [21.0, 21.0, np.nan],
        }
    )
    out = add_core_residuals(df)
    assert out["temperature_residual_c"].iloc[0] == -1.0
    assert pd.isna(out["temperature_residual_c"].iloc[1])
    assert pd.isna(out["temperature_residual_c"].iloc[2])


def test_residuals_skip_when_station_column_missing() -> None:
    df = pd.DataFrame(
        {
            "base_temperature_c": [21.0, 21.0],
            "relative_humidity_pct": [60.0, 65.0],
            "base_relative_humidity_pct": [55.0, 60.0],
        }
    )
    out = add_core_residuals(df)
    assert "temperature_residual_c" not in out.columns
    assert "relative_humidity_residual_pct" in out.columns


def test_residuals_skip_when_baseline_column_missing() -> None:
    df = pd.DataFrame(
        {
            "temperature_c": [20.0, 21.5],
            "wind_speed_kmh": [10.0, 12.0],
            "base_wind_speed_kmh": [9.0, 11.0],
        }
    )
    out = add_core_residuals(df)
    assert "temperature_residual_c" not in out.columns
    assert "wind_speed_residual_kmh" in out.columns


def test_residuals_no_inputs_no_output() -> None:
    df = pd.DataFrame({"other": [1, 2, 3]})
    out = add_core_residuals(df)
    assert list(out.columns) == ["other"]


def test_residuals_does_not_mutate_input() -> None:
    df = make_full_frame()
    before = list(df.columns)
    add_core_residuals(df)
    assert list(df.columns) == before
