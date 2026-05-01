from __future__ import annotations

import numpy as np
import pandas as pd

from postprocessing.features.radiation import (
    CLEAR_SKY_REFERENCE_WM2,
    SHORTWAVE_UPPER_BOUND_WM2,
    add_cloud_attenuation,
    add_clipped_shortwave,
    add_radiation_features,
    add_solar_to_clear_sky_ratio,
    add_sunshine_fraction,
    add_uv_proxy,
)


def test_clipped_shortwave_clips_high() -> None:
    df = pd.DataFrame({"base_solar_radiation_wm2": [0.0, 500.0, 1500.0, 1800.0, 5000.0]})
    out = add_clipped_shortwave(df)
    assert out["clipped_shortwave_wm2"].tolist() == [0.0, 500.0, 1500.0, 1500.0, 1500.0]


def test_clipped_shortwave_clips_negative() -> None:
    df = pd.DataFrame({"base_solar_radiation_wm2": [-5.0, -0.1, 0.0, 100.0]})
    out = add_clipped_shortwave(df)
    assert out["clipped_shortwave_wm2"].tolist() == [0.0, 0.0, 0.0, 100.0]


def test_clipped_shortwave_no_op_when_missing() -> None:
    df = pd.DataFrame({"other": [1, 2]})
    out = add_clipped_shortwave(df)
    assert "clipped_shortwave_wm2" not in out.columns


def test_cloud_attenuation_full_range() -> None:
    df = pd.DataFrame({"base_cloud_cover_pct": [0, 25, 50, 100]})
    out = add_cloud_attenuation(df)
    assert out["cloud_attenuation_factor"].tolist() == [1.0, 0.75, 0.5, 0.0]


def test_cloud_attenuation_clips_overage() -> None:
    df = pd.DataFrame({"base_cloud_cover_pct": [-5, 105]})
    out = add_cloud_attenuation(df)
    assert out["cloud_attenuation_factor"].iloc[0] == 1.0
    assert out["cloud_attenuation_factor"].iloc[1] == 0.0


def test_sunshine_fraction_basic() -> None:
    df = pd.DataFrame({"base_sunshine_seconds": [0, 1800, 3600]})
    out = add_sunshine_fraction(df)
    assert out["sunshine_fraction"].tolist() == [0.0, 0.5, 1.0]


def test_sunshine_fraction_clips_overage() -> None:
    df = pd.DataFrame({"base_sunshine_seconds": [3601, 4000, -10]})
    out = add_sunshine_fraction(df)
    assert out["sunshine_fraction"].iloc[0] == 1.0
    assert out["sunshine_fraction"].iloc[1] == 1.0
    assert out["sunshine_fraction"].iloc[2] == 0.0


def test_uv_proxy_basic_multiplication() -> None:
    df = pd.DataFrame(
        {
            "clipped_shortwave_wm2": [800.0, 600.0, 0.0, 1000.0],
            "cloud_attenuation_factor": [1.0, 0.5, 1.0, 0.0],
            "sunshine_fraction": [1.0, 0.8, 1.0, 1.0],
        }
    )
    out = add_uv_proxy(df)
    assert np.isclose(out["uv_proxy"].iloc[0], 800.0)
    assert np.isclose(out["uv_proxy"].iloc[1], 240.0)
    assert np.isclose(out["uv_proxy"].iloc[2], 0.0)
    assert np.isclose(out["uv_proxy"].iloc[3], 0.0)


def test_uv_proxy_skips_when_inputs_missing() -> None:
    df = pd.DataFrame({"clipped_shortwave_wm2": [500.0]})
    out = add_uv_proxy(df)
    assert "uv_proxy" not in out.columns


def test_solar_to_clear_sky_ratio_basic() -> None:
    df = pd.DataFrame({"clipped_shortwave_wm2": [0.0, 500.0, 1000.0, 1500.0]})
    out = add_solar_to_clear_sky_ratio(df)
    assert np.isclose(out["solar_to_clear_sky_ratio"].iloc[0], 0.0)
    assert np.isclose(out["solar_to_clear_sky_ratio"].iloc[1], 0.5)
    assert np.isclose(out["solar_to_clear_sky_ratio"].iloc[2], 1.0)
    assert np.isclose(out["solar_to_clear_sky_ratio"].iloc[3], 1.5)


def test_constants_make_sense() -> None:
    assert SHORTWAVE_UPPER_BOUND_WM2 == 1500.0
    assert CLEAR_SKY_REFERENCE_WM2 == 1000.0


def test_add_radiation_features_full_pipeline() -> None:
    df = pd.DataFrame(
        {
            "base_solar_radiation_wm2": [0.0, 500.0, 800.0],
            "base_cloud_cover_pct": [0, 25, 50],
            "base_sunshine_seconds": [0, 1800, 3600],
        }
    )
    out = add_radiation_features(df)
    for c in [
        "clipped_shortwave_wm2",
        "cloud_attenuation_factor",
        "sunshine_fraction",
        "uv_proxy",
        "solar_to_clear_sky_ratio",
    ]:
        assert c in out.columns


def test_add_radiation_features_partial_inputs() -> None:
    df = pd.DataFrame({"base_solar_radiation_wm2": [500.0]})
    out = add_radiation_features(df)
    assert "clipped_shortwave_wm2" in out.columns
    assert "solar_to_clear_sky_ratio" in out.columns
    assert "cloud_attenuation_factor" not in out.columns
    assert "uv_proxy" not in out.columns
