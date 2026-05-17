from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from postprocessing.training.data_loader import (
    known_targets,
    loso_split,
    predicts_residual,
    prepare_target_frame,
    target_columns_for,
    walk_forward_splits,
    within_station_hourly_split,
)


def _make_synthetic(n_stations: int = 3, n_hours: int = 200, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    base_time = pd.Timestamp("2025-07-01 00:00:00", tz="UTC")
    for s in range(n_stations):
        sid = f"S{s:02d}"
        for h in range(n_hours):
            rows.append({
                "station_id": sid,
                "valid_time_utc": base_time + pd.Timedelta(hours=h),
                "elevation_m": 100.0 + 100.0 * s,
                "station_latitude": 30.0 + s,
                "station_longitude": 15.0 + s,
                "temperature_c": 20.0 + rng.normal(),
                "base_temperature_c": 19.0 + rng.normal(),
                "temperature_residual_c": rng.normal(),
                "hour_sin": np.sin(2 * np.pi * h / 24),
                "hour_cos": np.cos(2 * np.pi * h / 24),
                "is_day_flag": int(6 <= (h % 24) < 18),
                "gate_temperature_ready": True,
            })
    return pd.DataFrame(rows)


def test_known_targets_matches_expected_set():
    expected = {
        "temperature", "relative_humidity", "dew_point",
        "wind_speed", "wind_gust", "pressure",
        "wind_direction", "rain_occurrence", "rain_amount", "uv",
    }
    assert set(known_targets()) == expected


def test_target_columns_for_temperature():
    assert target_columns_for("temperature") == ("temperature_residual_c",)


def test_target_columns_for_pressure_uses_max_residual():
    assert target_columns_for("pressure") == ("pressure_residual_max_hpa",)


def test_target_columns_for_wind_direction_is_sin_cos():
    cols = target_columns_for("wind_direction")
    assert "winddir_residual_sin" in cols
    assert "winddir_residual_cos" in cols


def test_target_columns_for_unknown_raises():
    with pytest.raises(KeyError):
        target_columns_for("not_a_target")


def test_predicts_residual_flags_uv_as_absolute():
    assert predicts_residual("temperature")
    assert predicts_residual("pressure")
    assert not predicts_residual("uv")
    assert not predicts_residual("rain_occurrence")


def test_prepare_target_frame_creates_leaded_columns():
    df = _make_synthetic()
    out = prepare_target_frame(df, "temperature", leads=(1, 6), apply_overrides=False)
    assert "temperature_residual_c_lead_1h" in out.columns
    assert "temperature_residual_c_lead_6h" in out.columns
    assert "base_temperature_c_lead_1h" in out.columns
    assert "hour_sin_lead_1h" in out.columns
    assert "gate_temperature_ready_lead_1h" in out.columns
    assert "issue_time_utc" in out.columns


def test_prepare_target_frame_empty_leads_raises():
    df = _make_synthetic()
    with pytest.raises(ValueError):
        prepare_target_frame(df, "temperature", leads=(), apply_overrides=False)


def test_prepare_target_frame_filters_failed_gate():
    df = _make_synthetic()
    df.loc[df["station_id"] == "S00", "gate_temperature_ready"] = False
    out = prepare_target_frame(df, "temperature", leads=(1,), apply_overrides=False)
    assert "S00" not in set(out["station_id"].unique())


def test_prepare_target_frame_no_cross_station_leakage():
    df = _make_synthetic(n_stations=2, n_hours=10)
    df.loc[df["station_id"] == "S00", "temperature_residual_c"] = 1.0
    df.loc[df["station_id"] == "S01", "temperature_residual_c"] = 2.0
    out = prepare_target_frame(df, "temperature", leads=(1,), apply_overrides=False)
    for sid, expected in (("S00", 1.0), ("S01", 2.0)):
        observed = out.loc[out["station_id"] == sid, "temperature_residual_c_lead_1h"].dropna().unique()
        assert all(v == expected for v in observed), f"Cross-station leak for {sid}: {observed}"


def test_prepare_target_frame_applies_override(monkeypatch):
    df = _make_synthetic(n_stations=3, n_hours=50)
    fake_overrides = [
        {"station_id": "S01", "target": "temperature", "status": "unusable"},
    ]
    monkeypatch.setattr(
        "postprocessing.training.data_loader.load_overrides",
        lambda *_args, **_kwargs: fake_overrides,
    )
    out_with = prepare_target_frame(df, "temperature", leads=(1,), apply_overrides=True)
    out_without = prepare_target_frame(df, "temperature", leads=(1,), apply_overrides=False)
    assert "S01" not in set(out_with["station_id"].unique())
    assert "S01" in set(out_without["station_id"].unique())


def test_loso_split_excludes_holdout_station():
    df = _make_synthetic(n_stations=3, n_hours=50)
    train_idx, val_idx = loso_split(df, "S01")
    assert df.iloc[train_idx]["station_id"].isin(["S00", "S02"]).all()
    assert (df.iloc[val_idx]["station_id"] == "S01").all()
    assert len(train_idx) + len(val_idx) == len(df)
    assert set(train_idx).isdisjoint(set(val_idx))


def test_loso_split_unknown_station_raises():
    df = _make_synthetic(n_stations=2, n_hours=20)
    with pytest.raises(ValueError):
        loso_split(df, "S99")


def test_within_station_hourly_split_each_station_in_both():
    df = _make_synthetic(n_stations=3, n_hours=240)
    df = df.rename(columns={"valid_time_utc": "issue_time_utc"})
    train_idx, val_idx = within_station_hourly_split(df, fraction=0.10, block_hours=48, seed=42)
    train_stations = set(df.iloc[train_idx]["station_id"].unique())
    val_stations = set(df.iloc[val_idx]["station_id"].unique())
    assert train_stations == {"S00", "S01", "S02"}
    assert val_stations == {"S00", "S01", "S02"}


def test_within_station_hourly_split_disjoint_and_complete():
    df = _make_synthetic(n_stations=2, n_hours=240)
    df = df.rename(columns={"valid_time_utc": "issue_time_utc"})
    train_idx, val_idx = within_station_hourly_split(df, fraction=0.10, block_hours=48, seed=42)
    assert set(train_idx).isdisjoint(set(val_idx))
    assert len(train_idx) + len(val_idx) == len(df)


def test_within_station_hourly_split_seeded_reproducible():
    df = _make_synthetic(n_stations=2, n_hours=240)
    df = df.rename(columns={"valid_time_utc": "issue_time_utc"})
    a_train, a_val = within_station_hourly_split(df, fraction=0.10, block_hours=48, seed=42)
    b_train, b_val = within_station_hourly_split(df, fraction=0.10, block_hours=48, seed=42)
    c_train, c_val = within_station_hourly_split(df, fraction=0.10, block_hours=48, seed=99)
    assert np.array_equal(a_train, b_train)
    assert np.array_equal(a_val, b_val)
    assert not np.array_equal(a_val, c_val)


def test_walk_forward_splits_time_ordered():
    df = _make_synthetic(n_stations=2, n_hours=240)
    df = df.rename(columns={"valid_time_utc": "issue_time_utc"})
    folds = list(walk_forward_splits(df, n_folds=5))
    assert len(folds) >= 2
    for train_idx, val_idx in folds:
        train_max = df.iloc[train_idx]["issue_time_utc"].max()
        val_min = df.iloc[val_idx]["issue_time_utc"].min()
        assert train_max < val_min


def test_walk_forward_splits_train_grows_monotonically():
    df = _make_synthetic(n_stations=2, n_hours=240)
    df = df.rename(columns={"valid_time_utc": "issue_time_utc"})
    folds = list(walk_forward_splits(df, n_folds=5))
    train_sizes = [len(t) for t, _ in folds]
    for a, b in zip(train_sizes, train_sizes[1:]):
        assert b >= a


def test_walk_forward_splits_n_folds_validation():
    df = _make_synthetic(n_stations=1, n_hours=24)
    df = df.rename(columns={"valid_time_utc": "issue_time_utc"})
    with pytest.raises(ValueError):
        list(walk_forward_splits(df, n_folds=1))
