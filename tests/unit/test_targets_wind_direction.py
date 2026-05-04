from __future__ import annotations

import numpy as np
import pandas as pd

from postprocessing.targets.wind_direction import add_wind_direction_residual


def test_residual_basic_subtraction() -> None:
    df = pd.DataFrame(
        {
            "wind_direction_deg": [50.0, 100.0, 200.0],
            "base_wind_direction_deg": [30.0, 90.0, 195.0],
        }
    )
    out = add_wind_direction_residual(df)
    assert out["winddir_residual_deg"].tolist() == [20.0, 10.0, 5.0]


def test_residual_wraps_across_zero() -> None:
    df = pd.DataFrame(
        {
            "wind_direction_deg": [5.0, 355.0, 10.0],
            "base_wind_direction_deg": [355.0, 5.0, 350.0],
        }
    )
    out = add_wind_direction_residual(df)
    assert out["winddir_residual_deg"].iloc[0] == 10.0
    assert out["winddir_residual_deg"].iloc[1] == -10.0
    assert out["winddir_residual_deg"].iloc[2] == 20.0


def test_residual_in_signed_180_range() -> None:
    df = pd.DataFrame(
        {
            "wind_direction_deg": [0.0, 90.0, 180.0, 270.0, 359.0],
            "base_wind_direction_deg": [180.0, 270.0, 0.0, 90.0, 1.0],
        }
    )
    out = add_wind_direction_residual(df)
    assert (out["winddir_residual_deg"] >= -180.0).all()
    assert (out["winddir_residual_deg"] < 180.0).all() or (
        out["winddir_residual_deg"].abs() == 180.0
    ).any()


def test_sin_cos_match_residual() -> None:
    df = pd.DataFrame(
        {
            "wind_direction_deg": [0.0, 90.0, 5.0],
            "base_wind_direction_deg": [0.0, 0.0, 355.0],
        }
    )
    out = add_wind_direction_residual(df)
    assert np.isclose(out["winddir_residual_sin"].iloc[0], 0.0)
    assert np.isclose(out["winddir_residual_cos"].iloc[0], 1.0)
    assert np.isclose(out["winddir_residual_sin"].iloc[1], 1.0)
    assert np.isclose(out["winddir_residual_cos"].iloc[1], 0.0, atol=1e-10)
    assert np.isclose(
        out["winddir_residual_sin"].iloc[2],
        np.sin(np.deg2rad(10.0)),
    )


def test_sin_cos_close_for_nearly_equal_angles() -> None:
    df = pd.DataFrame(
        {
            "wind_direction_deg": [359.0, 1.0],
            "base_wind_direction_deg": [1.0, 359.0],
        }
    )
    out = add_wind_direction_residual(df)
    for i in range(2):
        distance_in_unit_circle = np.sqrt(
            out["winddir_residual_sin"].iloc[i] ** 2
            + (1 - out["winddir_residual_cos"].iloc[i]) ** 2
        )
        assert distance_in_unit_circle < 0.1


def test_sin_squared_plus_cos_squared_is_one() -> None:
    df = pd.DataFrame(
        {
            "wind_direction_deg": [37.0, 145.0, 270.0, 350.0],
            "base_wind_direction_deg": [10.0, 200.0, 100.0, 5.0],
        }
    )
    out = add_wind_direction_residual(df)
    reconstructed = (
        out["winddir_residual_sin"] ** 2 + out["winddir_residual_cos"] ** 2
    )
    assert np.allclose(reconstructed, 1.0)


def test_residual_skips_when_station_missing() -> None:
    df = pd.DataFrame({"base_wind_direction_deg": [0.0, 90.0]})
    out = add_wind_direction_residual(df)
    assert "winddir_residual_deg" not in out.columns
    assert "winddir_residual_sin" not in out.columns
    assert "winddir_residual_cos" not in out.columns


def test_residual_skips_when_baseline_missing() -> None:
    df = pd.DataFrame({"wind_direction_deg": [0.0, 90.0]})
    out = add_wind_direction_residual(df)
    assert "winddir_residual_deg" not in out.columns


def test_residual_propagates_nan() -> None:
    df = pd.DataFrame(
        {
            "wind_direction_deg": [50.0, np.nan, 200.0],
            "base_wind_direction_deg": [30.0, 90.0, np.nan],
        }
    )
    out = add_wind_direction_residual(df)
    assert out["winddir_residual_deg"].iloc[0] == 20.0
    assert pd.isna(out["winddir_residual_deg"].iloc[1])
    assert pd.isna(out["winddir_residual_sin"].iloc[2])


def test_does_not_mutate_input() -> None:
    df = pd.DataFrame(
        {
            "wind_direction_deg": [50.0],
            "base_wind_direction_deg": [30.0],
        }
    )
    before = list(df.columns)
    add_wind_direction_residual(df)
    assert list(df.columns) == before
