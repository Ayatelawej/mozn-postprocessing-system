from __future__ import annotations

import numpy as np
import pandas as pd

from postprocessing.targets.rain import (
    RAIN_OCCURRENCE_THRESHOLD_MM,
    add_rain_targets,
)


def test_threshold_constant_value() -> None:
    assert RAIN_OCCURRENCE_THRESHOLD_MM == 0.1


def test_targets_no_op_when_rain_missing() -> None:
    df = pd.DataFrame({"other": [1, 2, 3]})
    out = add_rain_targets(df)
    assert "rain_occurrence" not in out.columns
    assert "rain_amount_log1p" not in out.columns
    assert list(out.columns) == ["other"]


def test_occurrence_strict_greater_than() -> None:
    df = pd.DataFrame({"rain_total_mm": [0.0, 0.1, 0.10001, 0.5, 5.0]})
    out = add_rain_targets(df)
    assert out["rain_occurrence"].tolist() == [0, 0, 1, 1, 1]


def test_occurrence_dtype_is_int8() -> None:
    df = pd.DataFrame({"rain_total_mm": [0.0, 0.5]})
    out = add_rain_targets(df)
    assert str(out["rain_occurrence"].dtype) in ("int8", "Int8", "object")


def test_occurrence_preserves_nan() -> None:
    df = pd.DataFrame({"rain_total_mm": [0.0, 0.5, np.nan, 1.5]})
    out = add_rain_targets(df)
    assert out["rain_occurrence"].iloc[0] == 0
    assert out["rain_occurrence"].iloc[1] == 1
    assert pd.isna(out["rain_occurrence"].iloc[2])
    assert out["rain_occurrence"].iloc[3] == 1


def test_amount_log1p_zero_when_dry() -> None:
    df = pd.DataFrame({"rain_total_mm": [0.0, 0.0, 0.0]})
    out = add_rain_targets(df)
    assert out["rain_amount_log1p"].tolist() == [0.0, 0.0, 0.0]


def test_amount_log1p_correct_math() -> None:
    df = pd.DataFrame({"rain_total_mm": [0.0, 1.0, np.e - 1, 9.0]})
    out = add_rain_targets(df)
    assert np.isclose(out["rain_amount_log1p"].iloc[0], 0.0)
    assert np.isclose(out["rain_amount_log1p"].iloc[1], np.log(2))
    assert np.isclose(out["rain_amount_log1p"].iloc[2], 1.0)
    assert np.isclose(out["rain_amount_log1p"].iloc[3], np.log(10))


def test_amount_log1p_clips_negatives() -> None:
    df = pd.DataFrame({"rain_total_mm": [-0.05, -1.0, 0.5]})
    out = add_rain_targets(df)
    assert out["rain_amount_log1p"].iloc[0] == 0.0
    assert out["rain_amount_log1p"].iloc[1] == 0.0
    assert np.isclose(out["rain_amount_log1p"].iloc[2], np.log1p(0.5))


def test_amount_log1p_propagates_nan() -> None:
    df = pd.DataFrame({"rain_total_mm": [1.0, np.nan, 3.0]})
    out = add_rain_targets(df)
    assert np.isclose(out["rain_amount_log1p"].iloc[0], np.log(2))
    assert pd.isna(out["rain_amount_log1p"].iloc[1])
    assert np.isclose(out["rain_amount_log1p"].iloc[2], np.log(4))


def test_amount_mm_is_passthrough() -> None:
    df = pd.DataFrame({"rain_total_mm": [0.0, 1.5, np.nan, 12.3]})
    out = add_rain_targets(df)
    assert out["rain_amount_mm"].iloc[0] == 0.0
    assert out["rain_amount_mm"].iloc[1] == 1.5
    assert pd.isna(out["rain_amount_mm"].iloc[2])
    assert out["rain_amount_mm"].iloc[3] == 12.3


def test_does_not_mutate_input() -> None:
    df = pd.DataFrame({"rain_total_mm": [0.0, 1.0]})
    before = list(df.columns)
    add_rain_targets(df)
    assert list(df.columns) == before
