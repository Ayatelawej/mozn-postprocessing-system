from __future__ import annotations

import pandas as pd

SHORTWAVE_UPPER_BOUND_WM2 = 1500.0
CLEAR_SKY_REFERENCE_WM2 = 1000.0


def add_clipped_shortwave(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "base_solar_radiation_wm2" not in out.columns:
        return out
    out["clipped_shortwave_wm2"] = out["base_solar_radiation_wm2"].clip(
        lower=0.0, upper=SHORTWAVE_UPPER_BOUND_WM2
    )
    return out


def add_cloud_attenuation(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "base_cloud_cover_pct" not in out.columns:
        return out
    attenuation = 1.0 - (out["base_cloud_cover_pct"] / 100.0)
    out["cloud_attenuation_factor"] = attenuation.clip(lower=0.0, upper=1.0)
    return out


def add_sunshine_fraction(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "base_sunshine_seconds" not in out.columns:
        return out
    fraction = out["base_sunshine_seconds"] / 3600.0
    out["sunshine_fraction"] = fraction.clip(lower=0.0, upper=1.0)
    return out


def add_uv_proxy(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    required = ["clipped_shortwave_wm2", "cloud_attenuation_factor", "sunshine_fraction"]
    if not all(c in out.columns for c in required):
        return out
    out["uv_proxy"] = (
        out["clipped_shortwave_wm2"]
        * out["cloud_attenuation_factor"]
        * out["sunshine_fraction"]
    )
    return out


def add_solar_to_clear_sky_ratio(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "clipped_shortwave_wm2" not in out.columns:
        return out
    ratio = out["clipped_shortwave_wm2"] / CLEAR_SKY_REFERENCE_WM2
    out["solar_to_clear_sky_ratio"] = ratio.clip(lower=0.0, upper=2.0)
    return out


def add_radiation_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df
    out = add_clipped_shortwave(out)
    out = add_cloud_attenuation(out)
    out = add_sunshine_fraction(out)
    out = add_uv_proxy(out)
    out = add_solar_to_clear_sky_ratio(out)
    return out
