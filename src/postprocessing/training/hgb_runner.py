from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor

from postprocessing.training.data_loader import (
    loso_split,
    predicts_residual,
    target_columns_for,
)
from postprocessing.training.feature_selection import features_for
from postprocessing.training.imputation import (
    ImputationStats,
    apply_imputation,
    compute_imputation_stats,
)
from postprocessing.training.preparation import prepare_for_target
from postprocessing.training.ridge_runner import (
    BASELINE_COLUMN,
    baseline_column_for,
)


@dataclass
class HGBConfig:
    max_depth: int
    learning_rate: float
    max_iter: int = 500
    early_stopping: bool = True
    validation_fraction: float = 0.15
    n_iter_no_change: int = 25
    random_state: int = 42

    def as_dict(self) -> dict:
        return asdict(self)

    def label(self) -> str:
        return f"d{self.max_depth}_lr{self.learning_rate}"


@dataclass
class HGBTrainResult:
    target: str
    lead: int
    holdout_station: str
    config_label: str
    config: dict
    n_train: int
    n_val: int
    n_iter_used: int
    feature_columns: list[str]
    missing_features: list[str]
    baseline_mae: float
    baseline_rmse: float
    baseline_bias: float
    corrected_mae: float
    corrected_rmse: float
    corrected_bias: float
    mae_reduction_pct: float
    rmse_reduction_pct: float
    bias_correction_pct: float

    def as_dict(self) -> dict:
        return asdict(self)


def fit_hgb(
    framed: pd.DataFrame,
    target: str,
    lead: int,
    holdout_station: str,
    *,
    config: HGBConfig,
) -> tuple[HGBTrainResult, dict]:
    if target not in BASELINE_COLUMN:
        raise NotImplementedError(
            f"hgb_runner.fit_hgb does not yet support target '{target}'"
        )

    target_col_base = target_columns_for(target)[0]
    target_col = f"{target_col_base}_lead_{lead}h"
    baseline_col = baseline_column_for(target)
    baseline_lead_col = f"{baseline_col}_lead_{lead}h"

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

    if predicts_residual(target):
        baseline_pred_full = np.zeros_like(y_val)
        baseline_pred_train_full = np.zeros_like(y_train)
    else:
        if baseline_lead_col not in val_df.columns:
            raise RuntimeError(
                f"Baseline column '{baseline_lead_col}' not in framed DataFrame for target '{target}'"
            )
        baseline_pred_full = val_df[baseline_lead_col].to_numpy(dtype=float)
        baseline_pred_train_full = train_df[baseline_lead_col].to_numpy(dtype=float)

    keep_train = (
        ~np.any(np.isnan(X_train), axis=1)
        & ~np.isnan(y_train)
        & ~np.isnan(baseline_pred_train_full)
    )
    keep_val = (
        ~np.any(np.isnan(X_val), axis=1)
        & ~np.isnan(y_val)
        & ~np.isnan(baseline_pred_full)
    )
    X_train, y_train = X_train[keep_train], y_train[keep_train]
    X_val, y_val = X_val[keep_val], y_val[keep_val]
    baseline_pred = baseline_pred_full[keep_val]

    if len(X_train) == 0:
        raise RuntimeError("No training rows remain after NaN filter")
    if len(X_val) == 0:
        raise RuntimeError(f"No validation rows for holdout '{holdout_station}'")

    model = HistGradientBoostingRegressor(
        max_depth=config.max_depth,
        learning_rate=config.learning_rate,
        max_iter=config.max_iter,
        early_stopping=config.early_stopping,
        validation_fraction=config.validation_fraction,
        n_iter_no_change=config.n_iter_no_change,
        random_state=config.random_state,
    )
    model.fit(X_train, y_train)
    y_pred = model.predict(X_val)
    n_iter_used = int(getattr(model, "n_iter_", config.max_iter))

    baseline_errors = y_val - baseline_pred
    corrected_errors = y_val - y_pred

    baseline_mae = float(np.mean(np.abs(baseline_errors)))
    corrected_mae = float(np.mean(np.abs(corrected_errors)))
    baseline_rmse = float(np.sqrt(np.mean(baseline_errors ** 2)))
    corrected_rmse = float(np.sqrt(np.mean(corrected_errors ** 2)))
    baseline_bias = float(np.mean(baseline_errors))
    corrected_bias = float(np.mean(corrected_errors))

    mae_red = 100.0 * (baseline_mae - corrected_mae) / baseline_mae if baseline_mae > 0 else 0.0
    rmse_red = 100.0 * (baseline_rmse - corrected_rmse) / baseline_rmse if baseline_rmse > 0 else 0.0
    if abs(baseline_bias) > 1e-9:
        bias_corr = 100.0 * (abs(baseline_bias) - abs(corrected_bias)) / abs(baseline_bias)
    else:
        bias_corr = 0.0

    result = HGBTrainResult(
        target=target,
        lead=lead,
        holdout_station=holdout_station,
        config_label=config.label(),
        config=config.as_dict(),
        n_train=int(len(X_train)),
        n_val=int(len(X_val)),
        n_iter_used=n_iter_used,
        feature_columns=present_features,
        missing_features=missing_features,
        baseline_mae=baseline_mae,
        baseline_rmse=baseline_rmse,
        baseline_bias=baseline_bias,
        corrected_mae=corrected_mae,
        corrected_rmse=corrected_rmse,
        corrected_bias=corrected_bias,
        mae_reduction_pct=mae_red,
        rmse_reduction_pct=rmse_red,
        bias_correction_pct=bias_corr,
    )

    artifact = {
        "model": model,
        "scaler": None,
        "imputation_stats": stats,
        "feature_columns": present_features,
        "missing_features": missing_features,
        "target": target,
        "lead": lead,
        "baseline_column": baseline_col,
        "predicts_residual": predicts_residual(target),
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "holdout_station": holdout_station,
        "config": config.as_dict(),
        "config_label": config.label(),
        "n_iter_used": n_iter_used,
        "model_class": "HistGradientBoostingRegressor",
    }

    return result, artifact


def train_hgb(
    canonical: pd.DataFrame,
    target: str,
    lead: int,
    holdout_station: str,
    *,
    config: HGBConfig,
) -> tuple[HGBTrainResult, dict]:
    if target not in BASELINE_COLUMN:
        raise NotImplementedError(
            f"hgb_runner.fit_hgb does not yet support target '{target}'"
        )
    framed = prepare_for_target(canonical, target, lead)
    return fit_hgb(framed, target, lead, holdout_station, config=config)


def save_artifact(artifact: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, path)
