from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

from postprocessing.features.circular import circular_diff_deg
from postprocessing.training.data_loader import loso_split
from postprocessing.training.feature_selection import features_for
from postprocessing.training.hgb_runner import HGBConfig
from postprocessing.training.imputation import (
    apply_imputation,
    compute_imputation_stats,
)
from postprocessing.training.preparation import prepare_for_target


WIND_SPEED_FILTER_KMH: float = 3.0


@dataclass
class WindDirectionResult:
    target: str
    lead: int
    holdout_station: str
    model_class: str
    hp_label: str
    hp: dict
    n_train: int
    n_val: int
    n_val_filtered: int
    feature_columns: list[str]
    baseline_circular_mae_deg: float
    baseline_circular_rmse_deg: float
    baseline_circular_mae_filtered_deg: float
    corrected_circular_mae_deg: float
    corrected_circular_rmse_deg: float
    corrected_circular_mae_filtered_deg: float
    mae_reduction_pct: float
    mae_reduction_filtered_pct: float

    def as_dict(self) -> dict:
        return asdict(self)


def _angle_from_sin_cos(sin_vals: np.ndarray, cos_vals: np.ndarray) -> np.ndarray:
    angles = np.degrees(np.arctan2(sin_vals, cos_vals))
    return np.mod(angles, 360.0)


def _vectorized_circular_diff(predicted_deg: np.ndarray, actual_deg: np.ndarray) -> np.ndarray:
    diff = predicted_deg - actual_deg
    return np.mod(diff + 180.0, 360.0) - 180.0


def _circular_mae(predicted_deg: np.ndarray, actual_deg: np.ndarray) -> float:
    return float(np.mean(np.abs(_vectorized_circular_diff(predicted_deg, actual_deg))))


def _circular_rmse(predicted_deg: np.ndarray, actual_deg: np.ndarray) -> float:
    diffs = _vectorized_circular_diff(predicted_deg, actual_deg)
    return float(np.sqrt(np.mean(diffs ** 2)))


def _prepare_xy(
    framed: pd.DataFrame,
    lead: int,
    holdout_station: str,
):
    target = "wind_direction"
    sin_col = f"winddir_residual_sin_lead_{lead}h"
    cos_col = f"winddir_residual_cos_lead_{lead}h"
    base_sin_col = f"base_wind_direction_sin_lead_{lead}h"
    base_cos_col = f"base_wind_direction_cos_lead_{lead}h"

    feature_list = features_for(target, lead)
    present_features = [c for c in feature_list if c in framed.columns]

    train_idx, val_idx = loso_split(framed, holdout_station)
    train_df = framed.iloc[train_idx].copy()
    val_df = framed.iloc[val_idx].copy()

    stats = compute_imputation_stats(train_df, present_features)
    train_df = apply_imputation(train_df, stats)
    val_df = apply_imputation(val_df, stats)

    X_train = train_df[present_features].to_numpy(dtype=float)
    X_val = val_df[present_features].to_numpy(dtype=float)
    y_train_sin = train_df[sin_col].to_numpy(dtype=float)
    y_train_cos = train_df[cos_col].to_numpy(dtype=float)
    y_val_sin = val_df[sin_col].to_numpy(dtype=float)
    y_val_cos = val_df[cos_col].to_numpy(dtype=float)
    base_sin = val_df[base_sin_col].to_numpy(dtype=float) if base_sin_col in val_df.columns else np.full_like(y_val_sin, np.nan)
    base_cos = val_df[base_cos_col].to_numpy(dtype=float) if base_cos_col in val_df.columns else np.full_like(y_val_cos, np.nan)
    val_wind_speed = val_df["wind_speed_kmh"].to_numpy(dtype=float) if "wind_speed_kmh" in val_df.columns else np.full_like(y_val_sin, np.nan)

    keep_train = (
        ~np.any(np.isnan(X_train), axis=1)
        & ~np.isnan(y_train_sin) & ~np.isnan(y_train_cos)
    )
    keep_val = (
        ~np.any(np.isnan(X_val), axis=1)
        & ~np.isnan(y_val_sin) & ~np.isnan(y_val_cos)
        & ~np.isnan(base_sin) & ~np.isnan(base_cos)
        & ~np.isnan(val_wind_speed)
    )
    X_train = X_train[keep_train]
    y_train = np.column_stack([y_train_sin[keep_train], y_train_cos[keep_train]])
    X_val = X_val[keep_val]
    y_val_sin = y_val_sin[keep_val]
    y_val_cos = y_val_cos[keep_val]
    base_sin = base_sin[keep_val]
    base_cos = base_cos[keep_val]
    val_wind_speed = val_wind_speed[keep_val]

    if len(X_train) == 0:
        raise RuntimeError("No training rows remain after NaN filter")
    if len(X_val) == 0:
        raise RuntimeError(f"No validation rows for holdout '{holdout_station}'")

    return (
        X_train, y_train,
        X_val, y_val_sin, y_val_cos,
        base_sin, base_cos, val_wind_speed,
        present_features, stats,
    )


def _compute_metrics(
    actual_sin: np.ndarray,
    actual_cos: np.ndarray,
    pred_sin: np.ndarray,
    pred_cos: np.ndarray,
    base_sin: np.ndarray,
    base_cos: np.ndarray,
    val_wind_speed: np.ndarray,
):
    base_angle = _angle_from_sin_cos(base_sin, base_cos)

    actual_residual_angle_deg = np.degrees(np.arctan2(actual_sin, actual_cos))
    actual_angle = np.mod(base_angle + actual_residual_angle_deg, 360.0)

    pred_residual_angle_deg = np.degrees(np.arctan2(pred_sin, pred_cos))
    corrected_angle = np.mod(base_angle + pred_residual_angle_deg, 360.0)

    baseline_mae = _circular_mae(base_angle, actual_angle)
    baseline_rmse = _circular_rmse(base_angle, actual_angle)
    corrected_mae = _circular_mae(corrected_angle, actual_angle)
    corrected_rmse = _circular_rmse(corrected_angle, actual_angle)

    speed_mask = val_wind_speed >= WIND_SPEED_FILTER_KMH
    n_filtered = int(np.sum(speed_mask))
    if n_filtered > 0:
        baseline_mae_filt = _circular_mae(base_angle[speed_mask], actual_angle[speed_mask])
        corrected_mae_filt = _circular_mae(corrected_angle[speed_mask], actual_angle[speed_mask])
    else:
        baseline_mae_filt = float("nan")
        corrected_mae_filt = float("nan")

    return (
        baseline_mae, baseline_rmse,
        corrected_mae, corrected_rmse,
        baseline_mae_filt, corrected_mae_filt,
        n_filtered,
    )


def fit_ridge_wind_direction(
    framed: pd.DataFrame,
    lead: int,
    holdout_station: str,
    *,
    alpha: float = 1.0,
) -> tuple[WindDirectionResult, dict]:
    (X_train, y_train, X_val, y_val_sin, y_val_cos,
     base_sin, base_cos, val_wind_speed,
     present_features, stats) = _prepare_xy(framed, lead, holdout_station)

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)

    model = Ridge(alpha=alpha)
    model.fit(X_train_scaled, y_train)
    y_pred = model.predict(X_val_scaled)
    pred_sin = y_pred[:, 0]
    pred_cos = y_pred[:, 1]

    (baseline_mae, baseline_rmse,
     corrected_mae, corrected_rmse,
     baseline_mae_filt, corrected_mae_filt,
     n_filtered) = _compute_metrics(
        y_val_sin, y_val_cos, pred_sin, pred_cos,
        base_sin, base_cos, val_wind_speed,
    )

    mae_red = 100.0 * (baseline_mae - corrected_mae) / baseline_mae if baseline_mae > 0 else 0.0
    if not np.isnan(baseline_mae_filt) and baseline_mae_filt > 0:
        mae_red_filt = 100.0 * (baseline_mae_filt - corrected_mae_filt) / baseline_mae_filt
    else:
        mae_red_filt = float("nan")

    result = WindDirectionResult(
        target="wind_direction",
        lead=lead,
        holdout_station=holdout_station,
        model_class="Ridge",
        hp_label=f"alpha{alpha}",
        hp={"alpha": alpha},
        n_train=int(len(X_train)),
        n_val=int(len(X_val_scaled)),
        n_val_filtered=n_filtered,
        feature_columns=present_features,
        baseline_circular_mae_deg=baseline_mae,
        baseline_circular_rmse_deg=baseline_rmse,
        baseline_circular_mae_filtered_deg=baseline_mae_filt,
        corrected_circular_mae_deg=corrected_mae,
        corrected_circular_rmse_deg=corrected_rmse,
        corrected_circular_mae_filtered_deg=corrected_mae_filt,
        mae_reduction_pct=mae_red,
        mae_reduction_filtered_pct=mae_red_filt,
    )

    artifact = {
        "model": model,
        "scaler": scaler,
        "imputation_stats": stats,
        "feature_columns": present_features,
        "target": "wind_direction",
        "lead": lead,
        "predicts_residual": True,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "holdout_station": holdout_station,
        "alpha": alpha,
        "model_class": "Ridge",
        "output_layout": ["winddir_residual_sin", "winddir_residual_cos"],
    }

    return result, artifact


def fit_hgb_wind_direction(
    framed: pd.DataFrame,
    lead: int,
    holdout_station: str,
    *,
    config: HGBConfig,
) -> tuple[WindDirectionResult, dict]:
    (X_train, y_train, X_val, y_val_sin, y_val_cos,
     base_sin, base_cos, val_wind_speed,
     present_features, stats) = _prepare_xy(framed, lead, holdout_station)

    def _make_hgb():
        return HistGradientBoostingRegressor(
            max_depth=config.max_depth,
            learning_rate=config.learning_rate,
            max_iter=config.max_iter,
            early_stopping=config.early_stopping,
            validation_fraction=config.validation_fraction,
            n_iter_no_change=config.n_iter_no_change,
            random_state=config.random_state,
        )

    model_sin = _make_hgb()
    model_sin.fit(X_train, y_train[:, 0])
    model_cos = _make_hgb()
    model_cos.fit(X_train, y_train[:, 1])

    pred_sin = model_sin.predict(X_val)
    pred_cos = model_cos.predict(X_val)

    (baseline_mae, baseline_rmse,
     corrected_mae, corrected_rmse,
     baseline_mae_filt, corrected_mae_filt,
     n_filtered) = _compute_metrics(
        y_val_sin, y_val_cos, pred_sin, pred_cos,
        base_sin, base_cos, val_wind_speed,
    )

    mae_red = 100.0 * (baseline_mae - corrected_mae) / baseline_mae if baseline_mae > 0 else 0.0
    if not np.isnan(baseline_mae_filt) and baseline_mae_filt > 0:
        mae_red_filt = 100.0 * (baseline_mae_filt - corrected_mae_filt) / baseline_mae_filt
    else:
        mae_red_filt = float("nan")

    result = WindDirectionResult(
        target="wind_direction",
        lead=lead,
        holdout_station=holdout_station,
        model_class="HGB",
        hp_label=config.label(),
        hp=config.as_dict(),
        n_train=int(len(X_train)),
        n_val=int(len(X_val)),
        n_val_filtered=n_filtered,
        feature_columns=present_features,
        baseline_circular_mae_deg=baseline_mae,
        baseline_circular_rmse_deg=baseline_rmse,
        baseline_circular_mae_filtered_deg=baseline_mae_filt,
        corrected_circular_mae_deg=corrected_mae,
        corrected_circular_rmse_deg=corrected_rmse,
        corrected_circular_mae_filtered_deg=corrected_mae_filt,
        mae_reduction_pct=mae_red,
        mae_reduction_filtered_pct=mae_red_filt,
    )

    artifact = {
        "model_sin": model_sin,
        "model_cos": model_cos,
        "scaler": None,
        "imputation_stats": stats,
        "feature_columns": present_features,
        "target": "wind_direction",
        "lead": lead,
        "predicts_residual": True,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "holdout_station": holdout_station,
        "config": config.as_dict(),
        "config_label": config.label(),
        "model_class": "HistGradientBoostingRegressor",
        "output_layout": ["winddir_residual_sin", "winddir_residual_cos"],
    }

    return result, artifact


def save_artifact(artifact: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, path)
