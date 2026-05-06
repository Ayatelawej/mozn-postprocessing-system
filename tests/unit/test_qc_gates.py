from __future__ import annotations

import pandas as pd
import pytest

from postprocessing.qc.gates import (
    TRAINABILITY_GATE_COLUMNS,
    filter_to_trainable,
    gate_column_for,
    is_trainable,
    trainable_rows_per_target,
)


def make_validated_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "station_id": ["A", "A", "A", "B", "B"],
            "gate_temperature_ready": [True, True, False, True, False],
            "gate_pressure_ready": [True, False, False, False, False],
            "gate_wind_direction_ready": [True, True, True, True, True],
            "gate_rain_occurrence_ready": [False, False, False, False, False],
            "temperature_residual_c": [0.5, 0.6, 0.7, -0.1, -0.2],
        }
    )


def test_known_targets_have_gate_columns() -> None:
    expected = {
        "temperature",
        "relative_humidity",
        "dew_point",
        "wind_speed",
        "uv",
        "pressure",
        "wind_gust",
        "wind_direction",
        "rain_occurrence",
        "rain_amount",
    }
    assert set(TRAINABILITY_GATE_COLUMNS.keys()) == expected


def test_gate_column_for_known_target() -> None:
    assert gate_column_for("temperature") == "gate_temperature_ready"
    assert gate_column_for("rain_occurrence") == "gate_rain_occurrence_ready"


def test_gate_column_for_unknown_target_raises() -> None:
    with pytest.raises(KeyError):
        gate_column_for("does_not_exist")


def test_is_trainable_returns_bool_series() -> None:
    df = make_validated_frame()
    mask = is_trainable(df, "temperature")
    assert mask.dtype == bool
    assert mask.tolist() == [True, True, False, True, False]


def test_is_trainable_handles_nan_as_false() -> None:
    df = pd.DataFrame({"gate_temperature_ready": [True, None, False]})
    mask = is_trainable(df, "temperature")
    assert mask.tolist() == [True, False, False]


def test_is_trainable_raises_when_gate_column_missing() -> None:
    df = pd.DataFrame({"other": [1, 2, 3]})
    with pytest.raises(KeyError):
        is_trainable(df, "temperature")


def test_filter_to_trainable_basic() -> None:
    df = make_validated_frame()
    out = filter_to_trainable(df, "temperature")
    assert len(out) == 3
    assert out["station_id"].tolist() == ["A", "A", "B"]
    assert out["temperature_residual_c"].tolist() == [0.5, 0.6, -0.1]


def test_filter_to_trainable_zero_rows_when_all_fail() -> None:
    df = make_validated_frame()
    out = filter_to_trainable(df, "rain_occurrence")
    assert len(out) == 0


def test_filter_to_trainable_resets_index() -> None:
    df = make_validated_frame()
    out = filter_to_trainable(df, "temperature")
    assert list(out.index) == [0, 1, 2]


def test_trainable_rows_per_target_counts() -> None:
    df = make_validated_frame()
    counts = trainable_rows_per_target(df)
    assert counts["temperature"] == 3
    assert counts["pressure"] == 1
    assert counts["wind_direction"] == 5
    assert counts["rain_occurrence"] == 0


def test_trainable_rows_per_target_skips_missing_columns() -> None:
    df = pd.DataFrame({"gate_temperature_ready": [True, False, True]})
    counts = trainable_rows_per_target(df)
    assert counts == {"temperature": 2}
