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

from postprocessing.training.data_loader import loso_split
from postprocessing.training.feature_selection import features_for
from postprocessing.training.hgb_runner import HGBConfig
from postprocessing.training.imputation import (
    apply_imputation,
    compute_imputation_stats,
)


POSITIVE_RAIN_THRESHOLD_MM: float = 0.1


@dataclass
class RainAmountResult:
    target: str
    lead: int
    holdout_station: str
    model_class: str
    hp_label: str
    hp: dict
    n_train_total: int
    n_train_positive: int
    n_val_total: int
    n_val_positive: int
    feature_columns: list[str]
    baseline_mae_mm_on_positive: float
    baseline_rmse_mm_on_positive: float
    baseline_bias_mm_on_positive: float
    corrected_mae_mm_on_positive: float
    corrected_rmse_mm_on_positive: float
    corrected_bias_mm_on_positive: float
    mae_reduction_pct: float
    rmse_reduction_pct: float
    mean_actual_mm_on_positive: float
    median_actual_mm_on_positive: float
    p95_actual_mm_on_positive: float

    def as_dict(self) -> dict:
        return asdict(self)


def _with_leaded_mm(df: pd.DataFrame, target_col_log1p: str) -> pd.DataFrame:
    out = df.copy()
    out["__rain_amount_mm_at_lead"] = np.expm1(out[target_col_log1p].to_numpy(dtype=float))
    return out


def _prepare_xy_conditional(
    framed: pd.DataFrame,
    lead: int,
    holdout_station: str,
):
    target_col_log1p = f"rain_amount_log1p_lead_{lead}h"
    baseline_col = f"base_precipitation_mm_lead_{lead}h"

    feature_list = features_for("rain_amount", lead)
    present_features = [c for c in feature_list if c in framed.columns]

    train_idx, val_idx = loso_split(framed, holdout_station)
    train_df = framed.iloc[train_idx].copy()
    val_df = framed.iloc[val_idx].copy()

    n_train_total = int(len(train_df))
    n_val_total = int(len(val_df))

    train_df = _with_leaded_mm(train_df, target_col_log1p)
    val_df = _with_leaded_mm(val_df, target_col_log1p)
    train_df_pos = train_df[train_df["__rain_amount_mm_at_lead"] >= POSITIVE_RAIN_THRESHOLD_MM].copy()
    val_df_pos = val_df[val_df["__rain_amount_mm_at_lead"] >= POSITIVE_RAIN_THRESHOLD_MM].copy()

    if len(train_df_pos) < 50:
        raise RuntimeError(
            f"Too few positive training rows ({len(train_df_pos)}); "
            f"need at least 50 for stable fit"
        )
    if len(val_df_pos) == 0:
        raise RuntimeError(
            f"No positive validation rows for holdout '{holdout_station}'"
        )

    stats = compute_imputation_stats(train_df_pos, present_features)
    train_df_pos = apply_imputation(train_df_pos, stats)
    val_df_pos = apply_imputation(val_df_pos, stats)

    X_train = train_df_pos[present_features].to_numpy(dtype=float)
    X_val = val_df_pos[present_features].to_numpy(dtype=float)
    y_train_log1p = train_df_pos[target_col_log1p].to_numpy(dtype=float)
    y_val_mm = val_df_pos["__rain_amount_mm_at_lead"].to_numpy(dtype=float)
    base_val_mm = val_df_pos[baseline_col].to_numpy(dtype=float) if baseline_col in val_df_pos.columns else np.full_like(y_val_mm, np.nan)

    keep_train = (
        ~np.any(np.isnan(X_train), axis=1)
        & ~np.isnan(y_train_log1p)
    )
    keep_val = (
        ~np.any(np.isnan(X_val), axis=1)
        & ~np.isnan(y_val_mm)
        & ~np.isnan(base_val_mm)
    )
    X_train = X_train[keep_train]
    y_train_log1p = y_train_log1p[keep_train]
    X_val = X_val[keep_val]
    y_val_mm = y_val_mm[keep_val]
    base_val_mm = base_val_mm[keep_val]

    if len(X_train) < 50:
        raise RuntimeError(f"Too few training rows after NaN filter ({len(X_train)})")
    if len(X_val) == 0:
        raise RuntimeError(f"No validation rows after NaN filter for holdout '{holdout_station}'")

    return (
        X_train, y_train_log1p,
        X_val, y_val_mm,
        base_val_mm,
        n_train_total, len(X_train),
        n_val_total, len(X_val),
        present_features, stats,
    )


def _compute_metrics_mm(
    y_val_mm: np.ndarray,
    pred_log1p: np.ndarray,
    base_val_mm: np.ndarray,
):
    pred_mm = np.expm1(pred_log1p)
    pred_mm = np.clip(pred_mm, 0.0, None)

    baseline_err = y_val_mm - base_val_mm
    corrected_err = y_val_mm - pred_mm

    baseline_mae = float(np.mean(np.abs(baseline_err)))
    corrected_mae = float(np.mean(np.abs(corrected_err)))
    baseline_rmse = float(np.sqrt(np.mean(baseline_err ** 2)))
    corrected_rmse = float(np.sqrt(np.mean(corrected_err ** 2)))
    baseline_bias = float(np.mean(baseline_err))
    corrected_bias = float(np.mean(corrected_err))

    mae_red = 100.0 * (baseline_mae - corrected_mae) / baseline_mae if baseline_mae > 0 else 0.0
    rmse_red = 100.0 * (baseline_rmse - corrected_rmse) / baseline_rmse if baseline_rmse > 0 else 0.0

    actual_mean = float(np.mean(y_val_mm))
    actual_median = float(np.median(y_val_mm))
    actual_p95 = float(np.percentile(y_val_mm, 95))

    return (
        baseline_mae, baseline_rmse, baseline_bias,
        corrected_mae, corrected_rmse, corrected_bias,
        mae_red, rmse_red,
        actual_mean, actual_median, actual_p95,
    )


def fit_ridge_rain_amount(
    framed: pd.DataFrame,
    lead: int,
    holdout_station: str,
    *,
    alpha: float = 1.0,
) -> tuple[RainAmountResult, dict]:
    (X_train, y_train_log1p,
     X_val, y_val_mm, base_val_mm,
     n_train_total, n_train_positive,
     n_val_total, n_val_positive,
     present_features, stats) = _prepare_xy_conditional(framed, lead, holdout_station)

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)

    model = Ridge(alpha=alpha)
    model.fit(X_train_scaled, y_train_log1p)
    pred_log1p = model.predict(X_val_scaled)

    (baseline_mae, baseline_rmse, baseline_bias,
     corrected_mae, corrected_rmse, corrected_bias,
     mae_red, rmse_red,
     actual_mean, actual_median, actual_p95) = _compute_metrics_mm(
        y_val_mm, pred_log1p, base_val_mm
    )

    result = RainAmountResult(
        target="rain_amount",
        lead=lead,
        holdout_station=holdout_station,
        model_class="Ridge",
        hp_label=f"alpha{alpha}",
        hp={"alpha": alpha},
        n_train_total=n_train_total,
        n_train_positive=n_train_positive,
        n_val_total=n_val_total,
        n_val_positive=n_val_positive,
        feature_columns=present_features,
        baseline_mae_mm_on_positive=baseline_mae,
        baseline_rmse_mm_on_positive=baseline_rmse,
        baseline_bias_mm_on_positive=baseline_bias,
        corrected_mae_mm_on_positive=corrected_mae,
        corrected_rmse_mm_on_positive=corrected_rmse,
        corrected_bias_mm_on_positive=corrected_bias,
        mae_reduction_pct=mae_red,
        rmse_reduction_pct=rmse_red,
        mean_actual_mm_on_positive=actual_mean,
        median_actual_mm_on_positive=actual_median,
        p95_actual_mm_on_positive=actual_p95,
    )

    artifact = {
        "model": model,
        "scaler": scaler,
        "imputation_stats": stats,
        "feature_columns": present_features,
        "target": "rain_amount",
        "lead": lead,
        "predicts_residual": False,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "holdout_station": holdout_station,
        "alpha": alpha,
        "model_class": "Ridge",
        "output_layout": "log1p_mm",
        "conditional_on_rain": True,
        "positive_rain_threshold_mm": POSITIVE_RAIN_THRESHOLD_MM,
    }

    return result, artifact


def fit_hgb_rain_amount(
    framed: pd.DataFrame,
    lead: int,
    holdout_station: str,
    *,
    config: HGBConfig,
) -> tuple[RainAmountResult, dict]:
    (X_train, y_train_log1p,
     X_val, y_val_mm, base_val_mm,
     n_train_total, n_train_positive,
     n_val_total, n_val_positive,
     present_features, stats) = _prepare_xy_conditional(framed, lead, holdout_station)

    model = HistGradientBoostingRegressor(
        max_depth=config.max_depth,
        learning_rate=config.learning_rate,
        max_iter=config.max_iter,
        early_stopping=config.early_stopping,
        validation_fraction=config.validation_fraction,
        n_iter_no_change=config.n_iter_no_change,
        random_state=config.random_state,
    )
    model.fit(X_train, y_train_log1p)
    pred_log1p = model.predict(X_val)
    n_iter_used = int(getattr(model, "n_iter_", config.max_iter))

    (baseline_mae, baseline_rmse, baseline_bias,
     corrected_mae, corrected_rmse, corrected_bias,
     mae_red, rmse_red,
     actual_mean, actual_median, actual_p95) = _compute_metrics_mm(
        y_val_mm, pred_log1p, base_val_mm
    )

    result = RainAmountResult(
        target="rain_amount",
        lead=lead,
        holdout_station=holdout_station,
        model_class="HistGradientBoostingRegressor",
        hp_label=config.label(),
        hp={**config.as_dict(), "n_iter_used": n_iter_used},
        n_train_total=n_train_total,
        n_train_positive=n_train_positive,
        n_val_total=n_val_total,
        n_val_positive=n_val_positive,
        feature_columns=present_features,
        baseline_mae_mm_on_positive=baseline_mae,
        baseline_rmse_mm_on_positive=baseline_rmse,
        baseline_bias_mm_on_positive=baseline_bias,
        corrected_mae_mm_on_positive=corrected_mae,
        corrected_rmse_mm_on_positive=corrected_rmse,
        corrected_bias_mm_on_positive=corrected_bias,
        mae_reduction_pct=mae_red,
        rmse_reduction_pct=rmse_red,
        mean_actual_mm_on_positive=actual_mean,
        median_actual_mm_on_positive=actual_median,
        p95_actual_mm_on_positive=actual_p95,
    )

    artifact = {
        "model": model,
        "scaler": None,
        "imputation_stats": stats,
        "feature_columns": present_features,
        "target": "rain_amount",
        "lead": lead,
        "predicts_residual": False,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "holdout_station": holdout_station,
        "config": config.as_dict(),
        "config_label": config.label(),
        "n_iter_used": n_iter_used,
        "model_class": "HistGradientBoostingRegressor",
        "output_layout": "log1p_mm",
        "conditional_on_rain": True,
        "positive_rain_threshold_mm": POSITIVE_RAIN_THRESHOLD_MM,
    }

    return result, artifact


def save_artifact(artifact: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, path)
