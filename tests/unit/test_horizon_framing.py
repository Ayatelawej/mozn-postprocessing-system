from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from postprocessing.horizon.framing import (
    DEFAULT_LEADS_HOURS,
    add_leaded_targets,
    build_horizon_table,
)


def make_two_station_frame(periods: int = 100) -> pd.DataFrame:
    timestamps = pd.date_range("2025-10-09 00:00", periods=periods, freq="h", tz="UTC")
    return pd.DataFrame(
        {
            "station_id": ["A"] * periods + ["B"] * periods,
            "valid_time_utc": list(timestamps) + list(timestamps),
            "temperature_residual_c": list(range(periods)) + [v * 2 for v in range(periods)],
            "wind_speed_residual_kmh": list(range(periods)) + list(range(periods)),
            "feature_x": [1.0] * (2 * periods),
        }
    )


def test_default_leads_are_1_to_72() -> None:
    assert DEFAULT_LEADS_HOURS == tuple(range(1, 73))
    assert len(DEFAULT_LEADS_HOURS) == 72


def test_add_leaded_creates_one_column_per_lead() -> None:
    df = make_two_station_frame()
    out = add_leaded_targets(df, ["temperature_residual_c"], leads=(1, 2, 3))
    assert "temperature_residual_c_lead_1h" in out.columns
    assert "temperature_residual_c_lead_2h" in out.columns
    assert "temperature_residual_c_lead_3h" in out.columns


def test_add_leaded_skips_missing_targets() -> None:
    df = make_two_station_frame()
    out = add_leaded_targets(df, ["does_not_exist"], leads=(1,))
    assert "does_not_exist_lead_1h" not in out.columns


def test_add_leaded_lead_1_is_next_row_per_station() -> None:
    df = make_two_station_frame()
    out = add_leaded_targets(df, ["temperature_residual_c"], leads=(1, 24))
    a = out[out["station_id"] == "A"].reset_index(drop=True)
    for i in range(50):
        assert a.loc[i, "temperature_residual_c_lead_1h"] == a.loc[i + 1, "temperature_residual_c"]
    for i in range(50):
        assert a.loc[i, "temperature_residual_c_lead_24h"] == a.loc[i + 24, "temperature_residual_c"]


def test_add_leaded_last_rows_per_station_are_nan() -> None:
    df = make_two_station_frame(periods=100)
    out = add_leaded_targets(df, ["temperature_residual_c"], leads=(1, 72))
    a = out[out["station_id"] == "A"].reset_index(drop=True)
    assert pd.isna(a.loc[99, "temperature_residual_c_lead_1h"])
    for i in range(28, 100):
        assert pd.isna(a.loc[i, "temperature_residual_c_lead_72h"])


def test_add_leaded_no_cross_station_leakage() -> None:
    df = make_two_station_frame(periods=100)
    out = add_leaded_targets(df, ["temperature_residual_c"], leads=(1,))
    a = out[out["station_id"] == "A"].reset_index(drop=True)
    b = out[out["station_id"] == "B"].reset_index(drop=True)
    assert pd.isna(a.loc[99, "temperature_residual_c_lead_1h"])
    assert pd.isna(b.loc[99, "temperature_residual_c_lead_1h"])


def test_add_leaded_raises_when_keys_missing() -> None:
    df = pd.DataFrame({"temperature_residual_c": [1.0, 2.0, 3.0]})
    with pytest.raises(KeyError):
        add_leaded_targets(df, ["temperature_residual_c"], leads=(1,))


def test_build_horizon_renames_to_issue_time() -> None:
    df = make_two_station_frame()
    out = build_horizon_table(df, ["temperature_residual_c"], leads=(1, 2, 3))
    assert "issue_time_utc" in out.columns
    assert "valid_time_utc" not in out.columns


def test_build_horizon_drops_rows_with_all_nan_targets() -> None:
    df = make_two_station_frame(periods=100)
    out = build_horizon_table(df, ["temperature_residual_c"], leads=(72,), drop_incomplete=True)
    a = out[out["station_id"] == "A"]
    assert len(a) == 28


def test_build_horizon_keeps_all_rows_when_drop_disabled() -> None:
    df = make_two_station_frame(periods=100)
    out = build_horizon_table(df, ["temperature_residual_c"], leads=(72,), drop_incomplete=False)
    assert len(out) == 200


def test_build_horizon_handles_multiple_targets() -> None:
    df = make_two_station_frame()
    out = build_horizon_table(df, ["temperature_residual_c", "wind_speed_residual_kmh"], leads=(1, 24))
    assert "temperature_residual_c_lead_1h" in out.columns
    assert "temperature_residual_c_lead_24h" in out.columns
    assert "wind_speed_residual_kmh_lead_1h" in out.columns
    assert "wind_speed_residual_kmh_lead_24h" in out.columns


def test_build_horizon_preserves_features() -> None:
    df = make_two_station_frame()
    out = build_horizon_table(df, ["temperature_residual_c"], leads=(1,))
    assert "feature_x" in out.columns


def test_build_horizon_does_not_mutate_input() -> None:
    df = make_two_station_frame()
    before_cols = list(df.columns)
    build_horizon_table(df, ["temperature_residual_c"], leads=(1,))
    assert list(df.columns) == before_cols
