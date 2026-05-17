from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

from postprocessing.training.data_loader import (
    loso_split,
    predicts_residual,
    prepare_target_frame,
    target_columns_for,
)
from postprocessing.training.feature_selection import features_for
from postprocessing.training.imputation import (
    ImputationStats,
    apply_imputation,
    compute_imputation_stats,
)


BASELINE_COLUMN: dict[str, str] = {
    "temperature": "base_temperature_c",
    "relative_humidity": "base_relative_humidity_pct",
    "dew_point": "base_dew_point_c",
    "wind_speed": "base_wind_speed_kmh",
    "wind_gust": "base_wind_gust_kmh",
    "pressure": "base_msl_pressure_hpa",
}


@dataclass
class TrainResult:
    target: str
    lead: int
    holdout_station: str
    alpha: float
    n_train: int
    n_val: int
    feature_columns: list[str]
    missing_features: list[str]
    baseline_mae: float
    baseline_rmse: float
    corrected_mae: float
    corrected_rmse: float
    mae_reduction_pct: float
    rmse_reduction_pct: float

    def as_dict(self) -> dict:
        return asdict(self)


def baseline_column_for(target: str) -> str:
    if target not in BASELINE_COLUMN:
        raise KeyError(f"No baseline column registered for target '{target}'")
    return BASELINE_COLUMN[target]


def train_ridge(
    canonical: pd.DataFrame,
    target: str,
    lead: int,
    holdout_station: str,
    *,
    alpha: float = 1.0,
) -> tuple[TrainResult, dict]:
    if not predicts_residual(target):
        raise NotImplementedError(
            f"ridge_runner.train_ridge supports residual targets only; got '{target}'"
        )

    framed = prepare_target_frame(canonical, target, leads=(lead,))
    if len(framed) == 0:
        raise RuntimeError(f"No trainable rows for target '{target}'")

    target_col_base = target_columns_for(target)[0]
    target_col = f"{target_col_base}_lead_{lead}h"

    feature_list = features_for(target, lead)
    present_features = [c for c in feature_list if c in framed.columns]
    missing_features = [c for c in feature_list if c not in framed.columns]

    train_idx, val_idx = loso_split(framed, holdout_station)
    train_df = framed.iloc[train_idx].copy()
    val_df = framed.iloc[val_idx].copy()

    stats = compute_imputation_stats(train_df, present_features)
    train_df = apply_imputation(train_df, stats)
    val_df = apply_imputation(val_df, stats)

    X_train = train_df[present_features].to_numpy(dtype=float)
    X_val = val_df[present_features].to_numpy(dtype=float)
    y_train = train_df[target_col].to_numpy(dtype=float)
    y_val = val_df[target_col].to_numpy(dtype=float)

    keep_train = ~np.any(np.isnan(X_train), axis=1) & ~np.isnan(y_train)
    keep_val = ~np.any(np.isnan(X_val), axis=1) & ~np.isnan(y_val)
    X_train, y_train = X_train[keep_train], y_train[keep_train]
    X_val, y_val = X_val[keep_val], y_val[keep_val]

    if len(X_train) == 0:
        raise RuntimeError("No training rows remain after NaN filter")
    if len(X_val) == 0:
        raise RuntimeError(f"No validation rows for holdout '{holdout_station}'")

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)

    model = Ridge(alpha=alpha)
    model.fit(X_train_scaled, y_train)
    y_pred = model.predict(X_val_scaled)

    baseline_errors = y_val
    corrected_errors = y_val - y_pred

    baseline_mae = float(np.mean(np.abs(baseline_errors)))
    corrected_mae = float(np.mean(np.abs(corrected_errors)))
    baseline_rmse = float(np.sqrt(np.mean(baseline_errors ** 2)))
    corrected_rmse = float(np.sqrt(np.mean(corrected_errors ** 2)))

    mae_red = 100.0 * (baseline_mae - corrected_mae) / baseline_mae if baseline_mae > 0 else 0.0
    rmse_red = 100.0 * (baseline_rmse - corrected_rmse) / baseline_rmse if baseline_rmse > 0 else 0.0

    result = TrainResult(
        target=target,
        lead=lead,
        holdout_station=holdout_station,
        alpha=alpha,
        n_train=int(len(X_train)),
        n_val=int(len(X_val)),
        feature_columns=present_features,
        missing_features=missing_features,
        baseline_mae=baseline_mae,
        baseline_rmse=baseline_rmse,
        corrected_mae=corrected_mae,
        corrected_rmse=corrected_rmse,
        mae_reduction_pct=mae_red,
        rmse_reduction_pct=rmse_red,
    )

    artifact = {
        "model": model,
        "scaler": scaler,
        "imputation_stats": stats,
        "feature_columns": present_features,
        "missing_features": missing_features,
        "target": target,
        "lead": lead,
        "baseline_column": baseline_column_for(target),
        "predicts_residual": predicts_residual(target),
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "holdout_station": holdout_station,
        "alpha": alpha,
    }

    return result, artifact


def save_artifact(artifact: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, path)


def load_artifact(path: Path) -> dict:
    return joblib.load(path)


def save_result_json(result: TrainResult, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(result.as_dict(), f, indent=2)
