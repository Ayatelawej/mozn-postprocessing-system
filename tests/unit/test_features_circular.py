from __future__ import annotations

import numpy as np
import pandas as pd

from postprocessing.features.circular import (
    add_circular_wind_features,
    add_direction_sin_cos,
    add_wind_uv,
    circular_diff_deg,
    wrap_angle_deg,
)


def test_wrap_angle_zero() -> None:
    assert wrap_angle_deg(0.0) == 0.0


def test_wrap_angle_below_180() -> None:
    assert wrap_angle_deg(170.0) == 170.0


def test_wrap_angle_above_180_wraps_negative() -> None:
    assert wrap_angle_deg(190.0) == -170.0


def test_wrap_angle_at_360_wraps_to_zero() -> None:
    assert wrap_angle_deg(360.0) == 0.0


def test_wrap_angle_negative() -> None:
    assert wrap_angle_deg(-190.0) == 170.0


def test_wrap_angle_handles_series() -> None:
    s = pd.Series([0.0, 90.0, 180.0, 270.0, 360.0])
    out = wrap_angle_deg(s)
    expected = pd.Series([0.0, 90.0, -180.0, -90.0, 0.0])
    pd.testing.assert_series_equal(out, expected, check_dtype=False)


def test_circular_diff_zero_when_equal() -> None:
    assert circular_diff_deg(45.0, 45.0) == 0.0


def test_circular_diff_simple_subtraction() -> None:
    assert circular_diff_deg(50.0, 30.0) == 20.0


def test_circular_diff_wraps_across_zero() -> None:
    assert circular_diff_deg(5.0, 355.0) == 10.0
    assert circular_diff_deg(355.0, 5.0) == -10.0


def test_circular_diff_opposite_directions() -> None:
    result = circular_diff_deg(0.0, 180.0)
    assert abs(result) == 180.0


def test_add_direction_sin_cos_creates_two_columns() -> None:
    df = pd.DataFrame({"wind_direction_deg": [0.0, 90.0, 180.0, 270.0]})
    out = add_direction_sin_cos(df, "wind_direction_deg", "wd")
    assert "wd_sin" in out.columns
    assert "wd_cos" in out.columns
    assert np.isclose(out["wd_sin"].iloc[0], 0.0)
    assert np.isclose(out["wd_cos"].iloc[0], 1.0)
    assert np.isclose(out["wd_sin"].iloc[1], 1.0)
    assert np.isclose(out["wd_cos"].iloc[1], 0.0, atol=1e-10)
    assert np.isclose(out["wd_sin"].iloc[2], 0.0, atol=1e-10)
    assert np.isclose(out["wd_cos"].iloc[2], -1.0)


def test_add_direction_sin_cos_missing_column_no_op() -> None:
    df = pd.DataFrame({"other": [1, 2, 3]})
    out = add_direction_sin_cos(df, "wind_direction_deg", "wd")
    assert "wd_sin" not in out.columns
    assert list(out.columns) == ["other"]


def test_add_direction_sin_cos_does_not_mutate() -> None:
    df = pd.DataFrame({"wind_direction_deg": [0.0, 90.0]})
    before = list(df.columns)
    add_direction_sin_cos(df, "wind_direction_deg", "wd")
    assert list(df.columns) == before


def test_add_wind_uv_north_wind() -> None:
    df = pd.DataFrame({"speed": [10.0], "dir": [0.0]})
    out = add_wind_uv(df, "speed", "dir", "u", "v")
    assert np.isclose(out["u"].iloc[0], 0.0)
    assert np.isclose(out["v"].iloc[0], 10.0)


def test_add_wind_uv_east_wind() -> None:
    df = pd.DataFrame({"speed": [10.0], "dir": [90.0]})
    out = add_wind_uv(df, "speed", "dir", "u", "v")
    assert np.isclose(out["u"].iloc[0], 10.0)
    assert np.isclose(out["v"].iloc[0], 0.0, atol=1e-10)


def test_add_wind_uv_south_wind() -> None:
    df = pd.DataFrame({"speed": [10.0], "dir": [180.0]})
    out = add_wind_uv(df, "speed", "dir", "u", "v")
    assert np.isclose(out["u"].iloc[0], 0.0, atol=1e-10)
    assert np.isclose(out["v"].iloc[0], -10.0)


def test_add_wind_uv_uv_squared_sums_to_speed_squared() -> None:
    df = pd.DataFrame({"speed": [10.0, 25.0, 7.5], "dir": [37.0, 145.0, 270.0]})
    out = add_wind_uv(df, "speed", "dir", "u", "v")
    reconstructed = np.sqrt(out["u"] ** 2 + out["v"] ** 2)
    pd.testing.assert_series_equal(reconstructed, df["speed"], check_names=False)


def test_add_wind_uv_missing_speed_no_op() -> None:
    df = pd.DataFrame({"dir": [0.0]})
    out = add_wind_uv(df, "speed", "dir", "u", "v")
    assert "u" not in out.columns
    assert "v" not in out.columns


def test_add_circular_wind_features_full_pipeline() -> None:
    df = pd.DataFrame(
        {
            "wind_direction_deg": [0.0, 90.0, 180.0],
            "wind_speed_kmh": [5.0, 10.0, 7.5],
            "base_wind_direction_deg": [10.0, 100.0, 190.0],
            "base_wind_speed_kmh": [4.0, 12.0, 8.0],
        }
    )
    out = add_circular_wind_features(df)
    for c in [
        "station_wind_direction_sin",
        "station_wind_direction_cos",
        "base_wind_direction_sin",
        "base_wind_direction_cos",
        "station_wind_u_kmh",
        "station_wind_v_kmh",
        "base_wind_u_kmh",
        "base_wind_v_kmh",
    ]:
        assert c in out.columns


def test_add_circular_wind_features_only_station_side() -> None:
    df = pd.DataFrame(
        {
            "wind_direction_deg": [0.0, 90.0],
            "wind_speed_kmh": [5.0, 10.0],
        }
    )
    out = add_circular_wind_features(df)
    assert "station_wind_direction_sin" in out.columns
    assert "station_wind_u_kmh" in out.columns
    assert "base_wind_direction_sin" not in out.columns
    assert "base_wind_u_kmh" not in out.columns
