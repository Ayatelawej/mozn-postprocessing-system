from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from postprocessing.training.ridge_runner import (
    baseline_column_for,
    fit_ridge,
    prepare_for_target,
    train_ridge,
)


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


def test_baseline_column_for_temperature():
    assert baseline_column_for("temperature") == "base_temperature_c"


def test_baseline_column_for_pressure():
    assert baseline_column_for("pressure") == "base_msl_pressure_hpa"


def test_baseline_column_for_unknown_raises():
    with pytest.raises(KeyError):
        baseline_column_for("not_a_target")


def test_train_ridge_produces_valid_result_and_artifact():
    df = _make_synthetic_canonical(n_stations=4, n_hours=500, seed=0)
    result, artifact = train_ridge(df, "temperature", lead=1, holdout_station="S01", alpha=1.0)
    assert result.target == "temperature"
    assert result.lead == 1
    assert result.holdout_station == "S01"
    assert result.n_train > 0
    assert result.n_val > 0
    assert result.baseline_mae > 0
    assert result.corrected_mae >= 0
    assert isinstance(result.baseline_bias, float)
    assert isinstance(result.corrected_bias, float)
    assert isinstance(result.bias_correction_pct, float)
    assert "model" in artifact
    assert "scaler" in artifact
    assert "imputation_stats" in artifact
    assert artifact["target"] == "temperature"
    assert artifact["lead"] == 1
    assert artifact["baseline_column"] == "base_temperature_c"
    assert artifact["predicts_residual"] is True


def test_train_ridge_reduces_mae_on_learnable_signal():
    df = _make_synthetic_canonical(n_stations=4, n_hours=500, seed=42)
    result, _ = train_ridge(df, "temperature", lead=1, holdout_station="S02", alpha=0.1)
    assert result.corrected_mae < result.baseline_mae, (
        f"Ridge failed to reduce MAE: baseline={result.baseline_mae:.4f}, "
        f"corrected={result.corrected_mae:.4f}"
    )
    assert result.mae_reduction_pct > 50.0, (
        f"With residual-at-T feature, expected >50% MAE reduction on synthetic; "
        f"got {result.mae_reduction_pct:.2f}%"
    )


def test_train_ridge_rejects_non_residual_target():
    df = _make_synthetic_canonical()
    with pytest.raises(NotImplementedError):
        train_ridge(df, "uv", lead=1, holdout_station="S01")


def test_train_ridge_unknown_holdout_raises():
    df = _make_synthetic_canonical()
    with pytest.raises(ValueError):
        train_ridge(df, "temperature", lead=1, holdout_station="S99")


def test_prepare_for_target_and_fit_ridge_match_train_ridge():
    df = _make_synthetic_canonical(n_stations=4, n_hours=300, seed=7)
    direct, _ = train_ridge(df, "temperature", lead=1, holdout_station="S01", alpha=1.0)
    framed = prepare_for_target(df, "temperature", lead=1)
    composed, _ = fit_ridge(framed, "temperature", 1, "S01", alpha=1.0)
    assert direct.n_train == composed.n_train
    assert direct.n_val == composed.n_val
    assert abs(direct.corrected_mae - composed.corrected_mae) < 1e-9
    assert abs(direct.baseline_bias - composed.baseline_bias) < 1e-9


def test_bias_correction_pct_sign_and_zero_baseline():
    df = _make_synthetic_canonical(n_stations=4, n_hours=300, seed=3)
    result, _ = train_ridge(df, "temperature", lead=1, holdout_station="S02", alpha=0.1)
    if abs(result.baseline_bias) > 1e-9:
        if abs(result.corrected_bias) < abs(result.baseline_bias):
            assert result.bias_correction_pct > 0
        else:
            assert result.bias_correction_pct <= 0
