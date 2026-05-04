from __future__ import annotations

import numpy as np
import pandas as pd

from postprocessing.targets.pressure import (
    add_pressure_avg,
    add_pressure_residuals,
)


def make_full_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "pressure_max_hpa": [1015.0, 1010.0, 1020.0],
            "pressure_min_hpa": [1013.0, 1008.0, 1018.0],
            "pressure_trend_hpa": [0.5, -1.0, 0.0],
            "base_surface_pressure_hpa": [1014.0, 1009.5, 1019.5],
        }
    )


def test_pressure_avg_basic() -> None:
    df = make_full_frame()
    out = add_pressure_avg(df)
    assert "pressure_avg_hpa" in out.columns
    assert out["pressure_avg_hpa"].tolist() == [1014.0, 1009.0, 1019.0]


def test_pressure_avg_skips_when_max_missing() -> None:
    df = pd.DataFrame({"pressure_min_hpa": [1010.0]})
    out = add_pressure_avg(df)
    assert "pressure_avg_hpa" not in out.columns


def test_pressure_avg_skips_when_min_missing() -> None:
    df = pd.DataFrame({"pressure_max_hpa": [1015.0]})
    out = add_pressure_avg(df)
    assert "pressure_avg_hpa" not in out.columns


def test_pressure_residuals_all_three() -> None:
    df = make_full_frame()
    out = add_pressure_residuals(df)
    assert out["pressure_residual_max_hpa"].tolist() == [1.0, 0.5, 0.5]
    assert out["pressure_residual_min_hpa"].tolist() == [-1.0, -1.5, -1.5]
    assert np.allclose(out["pressure_residual_avg_hpa"], [0.0, -0.5, -0.5])


def test_pressure_residuals_skip_when_baseline_missing() -> None:
    df = pd.DataFrame(
        {
            "pressure_max_hpa": [1015.0],
            "pressure_min_hpa": [1013.0],
        }
    )
    out = add_pressure_residuals(df)
    assert "pressure_residual_max_hpa" not in out.columns
    assert "pressure_residual_min_hpa" not in out.columns
    assert "pressure_residual_avg_hpa" not in out.columns


def test_pressure_residuals_partial_station_columns() -> None:
    df = pd.DataFrame(
        {
            "pressure_max_hpa": [1015.0, 1010.0],
            "base_surface_pressure_hpa": [1014.0, 1009.5],
        }
    )
    out = add_pressure_residuals(df)
    assert "pressure_residual_max_hpa" in out.columns
    assert "pressure_residual_min_hpa" not in out.columns
    assert "pressure_residual_avg_hpa" not in out.columns


def test_pressure_residuals_propagate_nan() -> None:
    df = pd.DataFrame(
        {
            "pressure_max_hpa": [1015.0, np.nan, 1020.0],
            "pressure_min_hpa": [1013.0, 1008.0, np.nan],
            "base_surface_pressure_hpa": [1014.0, 1009.0, 1019.5],
        }
    )
    out = add_pressure_residuals(df)
    assert out["pressure_residual_max_hpa"].iloc[0] == 1.0
    assert pd.isna(out["pressure_residual_max_hpa"].iloc[1])
    assert pd.isna(out["pressure_residual_avg_hpa"].iloc[1])
    assert pd.isna(out["pressure_residual_avg_hpa"].iloc[2])


def test_pressure_does_not_use_msl_pressure() -> None:
    df = pd.DataFrame(
        {
            "pressure_max_hpa": [1015.0],
            "pressure_min_hpa": [1013.0],
            "base_surface_pressure_hpa": [1014.0],
            "base_msl_pressure_hpa": [1023.0],
        }
    )
    out = add_pressure_residuals(df)
    assert out["pressure_residual_max_hpa"].iloc[0] == 1.0


def test_pressure_does_not_mutate_input() -> None:
    df = make_full_frame()
    before = list(df.columns)
    add_pressure_residuals(df)
    assert list(df.columns) == before
