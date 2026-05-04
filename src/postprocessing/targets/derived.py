from __future__ import annotations

import numpy as np
import pandas as pd

HEAT_INDEX_MIN_TEMP_C = 26.7
WIND_CHILL_MAX_TEMP_C = 10.0
WIND_CHILL_MIN_WIND_KMH = 4.8


def _heat_index_fahrenheit(temp_f: pd.Series, rh_pct: pd.Series) -> pd.Series:
    t = temp_f
    r = rh_pct
    return (
        -42.379
        + 2.04901523 * t
        + 10.14333127 * r
        - 0.22475541 * t * r
        - 0.00683783 * t * t
        - 0.05481717 * r * r
        + 0.00122874 * t * t * r
        + 0.00085282 * t * r * r
        - 0.00000199 * t * t * r * r
    )


def add_heat_index(
    df: pd.DataFrame,
    temp_col: str,
    rh_col: str,
    output_col: str = "heat_index_c",
) -> pd.DataFrame:
    out = df.copy()
    if temp_col not in out.columns or rh_col not in out.columns:
        return out

    temp_c = out[temp_col]
    rh_pct = out[rh_col]

    temp_f = temp_c * 9.0 / 5.0 + 32.0
    hi_f = _heat_index_fahrenheit(temp_f, rh_pct)
    hi_c = (hi_f - 32.0) * 5.0 / 9.0

    in_domain = temp_c >= HEAT_INDEX_MIN_TEMP_C
    out[output_col] = np.where(in_domain, hi_c, temp_c)
    return out


def add_wind_chill(
    df: pd.DataFrame,
    temp_col: str,
    wind_col: str,
    output_col: str = "wind_chill_c",
) -> pd.DataFrame:
    out = df.copy()
    if temp_col not in out.columns or wind_col not in out.columns:
        return out

    temp_c = out[temp_col]
    wind_kmh = out[wind_col]

    wind_clipped = wind_kmh.clip(lower=WIND_CHILL_MIN_WIND_KMH)
    wind_pow = wind_clipped ** 0.16
    wc = (
        13.12
        + 0.6215 * temp_c
        - 11.37 * wind_pow
        + 0.3965 * temp_c * wind_pow
    )

    in_domain = (temp_c <= WIND_CHILL_MAX_TEMP_C) & (wind_kmh >= WIND_CHILL_MIN_WIND_KMH)
    out[output_col] = np.where(in_domain, wc, temp_c)
    return out


def add_derived_features(
    df: pd.DataFrame,
    temp_col: str = "temperature_c",
    rh_col: str = "relative_humidity_pct",
    wind_col: str = "wind_speed_kmh",
) -> pd.DataFrame:
    out = add_heat_index(df, temp_col, rh_col)
    out = add_wind_chill(out, temp_col, wind_col)
    return out
