from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from postprocessing.training.preparation import prepare_for_target


def _tiny_canonical() -> pd.DataFrame:
    rows = []
    base_time = pd.Timestamp("2025-07-01 00:00:00", tz="UTC")
    for s in range(2):
        for h in range(50):
            rows.append({
                "station_id": f"S{s:02d}",
                "valid_time_utc": base_time + pd.Timedelta(hours=h),
                "elevation_m": 100.0,
                "station_latitude": 30.0,
                "station_longitude": 15.0,
                "temperature_c": 22.0,
                "base_temperature_c": 21.0,
                "temperature_residual_c": 1.0,
                "hour_sin": 0.0,
                "hour_cos": 1.0,
                "is_day_flag": 1,
                "gate_temperature_ready": True,
            })
    return pd.DataFrame(rows)


def test_prepare_for_target_returns_framed_with_leaded_columns():
    df = _tiny_canonical()
    framed = prepare_for_target(df, "temperature", lead=1)
    assert "temperature_residual_c_lead_1h" in framed.columns
    assert "base_temperature_c_lead_1h" in framed.columns
    assert "issue_time_utc" in framed.columns
    assert len(framed) > 0


def test_prepare_for_target_raises_when_no_trainable_rows():
    df = _tiny_canonical()
    df["gate_temperature_ready"] = False
    with pytest.raises(RuntimeError, match="No trainable rows"):
        prepare_for_target(df, "temperature", lead=1)
