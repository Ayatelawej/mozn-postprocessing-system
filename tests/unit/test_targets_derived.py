from __future__ import annotations

import numpy as np
import pandas as pd

from postprocessing.targets.derived import (
    HEAT_INDEX_MIN_TEMP_C,
    WIND_CHILL_MAX_TEMP_C,
    WIND_CHILL_MIN_WIND_KMH,
    add_derived_features,
    add_heat_index,
    add_wind_chill,
)


def test_constants_match_nws() -> None:
    assert HEAT_INDEX_MIN_TEMP_C == 26.7
    assert WIND_CHILL_MAX_TEMP_C == 10.0
    assert WIND_CHILL_MIN_WIND_KMH == 4.8


def test_heat_index_below_threshold_returns_temperature() -> None:
    df = pd.DataFrame(
        {"temperature_c": [10.0, 20.0, 26.0], "relative_humidity_pct": [50.0, 70.0, 90.0]}
    )
    out = add_heat_index(df, "temperature_c", "relative_humidity_pct")
    assert out["heat_index_c"].iloc[0] == 10.0
    assert out["heat_index_c"].iloc[1] == 20.0
    assert out["heat_index_c"].iloc[2] == 26.0


def test_heat_index_at_30c_60pct_is_higher_than_temperature() -> None:
    df = pd.DataFrame({"temperature_c": [30.0], "relative_humidity_pct": [60.0]})
    out = add_heat_index(df, "temperature_c", "relative_humidity_pct")
    hi = out["heat_index_c"].iloc[0]
    assert hi > 30.0
    assert hi < 40.0


def test_heat_index_at_35c_high_humidity_well_above_temperature() -> None:
    df = pd.DataFrame({"temperature_c": [35.0], "relative_humidity_pct": [80.0]})
    out = add_heat_index(df, "temperature_c", "relative_humidity_pct")
    hi = out["heat_index_c"].iloc[0]
    assert hi > 45.0


def test_heat_index_skips_when_columns_missing() -> None:
    df = pd.DataFrame({"temperature_c": [30.0]})
    out = add_heat_index(df, "temperature_c", "relative_humidity_pct")
    assert "heat_index_c" not in out.columns


def test_heat_index_propagates_nan() -> None:
    df = pd.DataFrame(
        {"temperature_c": [30.0, np.nan, 35.0], "relative_humidity_pct": [60.0, 70.0, np.nan]}
    )
    out = add_heat_index(df, "temperature_c", "relative_humidity_pct")
    assert out["heat_index_c"].iloc[0] > 30.0
    assert pd.isna(out["heat_index_c"].iloc[1])
    assert pd.isna(out["heat_index_c"].iloc[2])


def test_wind_chill_above_threshold_returns_temperature() -> None:
    df = pd.DataFrame({"temperature_c": [15.0, 20.0, 25.0], "wind_speed_kmh": [20.0, 30.0, 40.0]})
    out = add_wind_chill(df, "temperature_c", "wind_speed_kmh")
    assert out["wind_chill_c"].tolist() == [15.0, 20.0, 25.0]


def test_wind_chill_low_wind_returns_temperature() -> None:
    df = pd.DataFrame({"temperature_c": [0.0, 5.0], "wind_speed_kmh": [0.0, 3.0]})
    out = add_wind_chill(df, "temperature_c", "wind_speed_kmh")
    assert out["wind_chill_c"].tolist() == [0.0, 5.0]


def test_wind_chill_in_domain_is_below_temperature() -> None:
    df = pd.DataFrame({"temperature_c": [0.0], "wind_speed_kmh": [30.0]})
    out = add_wind_chill(df, "temperature_c", "wind_speed_kmh")
    wc = out["wind_chill_c"].iloc[0]
    assert wc < 0.0


def test_wind_chill_handles_zero_wind() -> None:
    df = pd.DataFrame({"temperature_c": [-5.0], "wind_speed_kmh": [0.0]})
    out = add_wind_chill(df, "temperature_c", "wind_speed_kmh")
    assert out["wind_chill_c"].iloc[0] == -5.0


def test_wind_chill_skips_when_columns_missing() -> None:
    df = pd.DataFrame({"temperature_c": [5.0]})
    out = add_wind_chill(df, "temperature_c", "wind_speed_kmh")
    assert "wind_chill_c" not in out.columns


def test_add_derived_features_full_pipeline() -> None:
    df = pd.DataFrame(
        {
            "temperature_c": [30.0, 5.0],
            "relative_humidity_pct": [70.0, 80.0],
            "wind_speed_kmh": [10.0, 25.0],
        }
    )
    out = add_derived_features(df)
    assert "heat_index_c" in out.columns
    assert "wind_chill_c" in out.columns
    assert out["heat_index_c"].iloc[0] > 30.0
    assert out["wind_chill_c"].iloc[1] < 5.0


def test_add_derived_features_with_corrected_columns() -> None:
    df = pd.DataFrame(
        {
            "temperature_corrected_c": [32.0],
            "relative_humidity_corrected_pct": [75.0],
            "wind_speed_corrected_kmh": [10.0],
        }
    )
    out = add_derived_features(
        df,
        temp_col="temperature_corrected_c",
        rh_col="relative_humidity_corrected_pct",
        wind_col="wind_speed_corrected_kmh",
    )
    assert "heat_index_c" in out.columns


def test_does_not_mutate_input() -> None:
    df = pd.DataFrame(
        {"temperature_c": [30.0], "relative_humidity_pct": [70.0], "wind_speed_kmh": [10.0]}
    )
    before = list(df.columns)
    add_derived_features(df)
    assert list(df.columns) == before
