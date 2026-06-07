from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from postprocessing.training.hgb_runner import (
    HGBConfig,
    fit_hgb,
    train_hgb,
)
from postprocessing.training.preparation import prepare_for_target


def _make_synthetic_canonical(n_stations: int = 4, n_hours: int = 500, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    base_time = pd.Timestamp("2025-07-01 00:00:00", tz="UTC")
    elevations = [3.0, 50.0, 200.0, 800.0]
    for s in range(n_stations):
        sid = f"S{s:02d}"
        elev = elevations[s % len(elevations)]
        for h in range(n_hours):
            t = base_time + pd.Timedelta(hours=h)
            hour = h % 24
            hour_sin = float(np.sin(2 * np.pi * hour / 24))
            hour_cos = float(np.cos(2 * np.pi * hour / 24))
            base_t = 22.0 + 8.0 * hour_sin + 0.001 * elev + rng.normal(scale=0.3)
            station_bias = -0.5 - 0.002 * elev + 0.4 * hour_cos
            station_t = base_t + station_bias + rng.normal(scale=0.2)
            rows.append({
                "station_id": sid,
                "valid_time_utc": t,
                "elevation_m": elev,
                "station_latitude": 30.0 + s,
                "station_longitude": 15.0 + s,
                "temperature_c": station_t,
                "base_temperature_c": base_t,
                "base_relative_humidity_pct": 60.0 + rng.normal(scale=5.0),
                "base_dew_point_c": 15.0 + rng.normal(scale=2.0),
                "base_cloud_cover_pct": 30.0 + rng.normal(scale=10.0),
                "base_is_day": int(6 <= hour < 18),
                "hour_sin": hour_sin,
                "hour_cos": hour_cos,
                "is_day_flag": int(6 <= hour < 18),
                "minutes_from_sunrise": 60.0 * hour,
                "solar_progress_0_1": min(max((hour - 6) / 12.0, 0.0), 1.0),
                "solar_centered": min(max((hour - 6) / 12.0, 0.0), 1.0) - 0.5,
                "clipped_shortwave_wm2": max(0.0, 800.0 * hour_sin),
                "cloud_attenuation_factor": 0.7,
                "sunshine_fraction": 0.8,
                "uv_proxy": max(0.0, 600.0 * hour_sin),
                "solar_to_clear_sky_ratio": 0.8,
                "temperature_residual_c": station_t - base_t,
                "gate_temperature_ready": True,
            })
    df = pd.DataFrame(rows)
    for var in ("temperature_c", "relative_humidity_pct", "dew_point_c"):
        for lag in (1, 2, 3, 6, 12, 24):
            df[f"{var}_lag_{lag}h"] = 0.0
    for stat in ("mean", "std"):
        for w in (3, 6, 12, 24):
            df[f"temperature_c_roll_{stat}_{w}h"] = 0.0
    return df


def test_hgb_config_label_format():
    cfg = HGBConfig(max_depth=6, learning_rate=0.1)
    assert cfg.label() == "d6_lr0.1"


def test_train_hgb_produces_valid_result_and_artifact():
    df = _make_synthetic_canonical()
    cfg = HGBConfig(max_depth=6, learning_rate=0.1, max_iter=100)
    result, artifact = train_hgb(df, "temperature", lead=1, holdout_station="S01", config=cfg)
    assert result.target == "temperature"
    assert result.lead == 1
    assert result.holdout_station == "S01"
    assert result.n_train > 0
    assert result.n_val > 0
    assert result.n_iter_used > 0
    assert isinstance(result.baseline_bias, float)
    assert isinstance(result.corrected_bias, float)
    assert "model" in artifact
    assert artifact["model_class"] == "HistGradientBoostingRegressor"
    assert artifact["target"] == "temperature"
    assert artifact["baseline_column"] == "base_temperature_c"


def test_train_hgb_reduces_mae_on_learnable_signal():
    df = _make_synthetic_canonical(seed=42)
    cfg = HGBConfig(max_depth=6, learning_rate=0.1, max_iter=200)
    result, _ = train_hgb(df, "temperature", lead=1, holdout_station="S02", config=cfg)
    assert result.corrected_mae < result.baseline_mae
    assert result.mae_reduction_pct > 40.0


def test_train_hgb_rejects_unsupported_target():
    df = _make_synthetic_canonical()
    cfg = HGBConfig(max_depth=6, learning_rate=0.1)
    with pytest.raises(NotImplementedError):
        train_hgb(df, "rain_occurrence", lead=1, holdout_station="S01", config=cfg)


def test_fit_hgb_matches_train_hgb_on_same_frame():
    df = _make_synthetic_canonical(seed=7)
    cfg = HGBConfig(max_depth=6, learning_rate=0.1, max_iter=100)
    direct, _ = train_hgb(df, "temperature", lead=1, holdout_station="S01", config=cfg)
    framed = prepare_for_target(df, "temperature", 1)
    composed, _ = fit_hgb(framed, "temperature", 1, "S01", config=cfg)
    assert direct.n_train == composed.n_train
    assert direct.n_val == composed.n_val
    assert abs(direct.corrected_mae - composed.corrected_mae) < 1e-9
