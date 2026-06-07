from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.preprocessing import StandardScaler

from postprocessing.training.data_loader import loso_split
from postprocessing.training.feature_selection import features_for
from postprocessing.training.imputation import (
    apply_imputation,
    compute_imputation_stats,
)


THRESHOLDS: tuple[float, ...] = (0.1, 0.3, 0.5)
BASELINE_PRECIP_COLUMN: str = "base_precipitation_mm"
BASELINE_PRECIP_THRESHOLD_MM: float = 0.0


@dataclass
class LogRegConfig:
    C: float
    max_iter: int = 1000
    class_weight: str | None = "balanced"
    random_state: int = 42

    def as_dict(self) -> dict:
        return asdict(self)

    def label(self) -> str:
        cw = "bal" if self.class_weight == "balanced" else "none"
        return f"C{self.C}_{cw}"


@dataclass
class HGBClassifierConfig:
    max_depth: int
    learning_rate: float
    max_iter: int = 500
    early_stopping: bool = True
    validation_fraction: float = 0.15
    n_iter_no_change: int = 25
    class_weight: str | None = None
    random_state: int = 42

    def as_dict(self) -> dict:
        return asdict(self)

    def label(self) -> str:
        cw = "bal" if self.class_weight == "balanced" else "none"
        return f"d{self.max_depth}_lr{self.learning_rate}_{cw}"


@dataclass
class RainClassifierResult:
    target: str
    lead: int
    holdout_station: str
    model_class: str
    hp_label: str
    hp: dict
    n_train: int
    n_val: int
    positive_rate_train: float
    positive_rate_val: float
    feature_columns: list[str]
    baseline_precision: float
    baseline_recall: float
    baseline_f1: float
    baseline_accuracy: float
    roc_auc: float
    average_precision: float
    brier_score: float
    metrics_at_threshold: dict
    f1_at_03: float
    f1_improvement_at_03: float

    def as_dict(self) -> dict:
        return asdict(self)


def _prepare_xy(
    framed: pd.DataFrame,
    lead: int,
    holdout_station: str,
):
    target_col = f"rain_occurrence_lead_{lead}h"
    baseline_col = f"{BASELINE_PRECIP_COLUMN}_lead_{lead}h"

    feature_list = features_for("rain_occurrence", lead)
    present_features = [c for c in feature_list if c in framed.columns]

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
    base_train = train_df[baseline_col].to_numpy(dtype=float) if baseline_col in train_df.columns else np.full_like(y_train, np.nan)
    base_val = val_df[baseline_col].to_numpy(dtype=float) if baseline_col in val_df.columns else np.full_like(y_val, np.nan)

    keep_train = (
        ~np.any(np.isnan(X_train), axis=1)
        & ~np.isnan(y_train)
        & ~np.isnan(base_train)
    )
    keep_val = (
        ~np.any(np.isnan(X_val), axis=1)
        & ~np.isnan(y_val)
        & ~np.isnan(base_val)
    )
    X_train = X_train[keep_train]
    y_train = y_train[keep_train].astype(int)
    X_val = X_val[keep_val]
    y_val = y_val[keep_val].astype(int)
    base_val = base_val[keep_val]

    if len(X_train) == 0:
        raise RuntimeError("No training rows remain after NaN filter")
    if len(X_val) == 0:
        raise RuntimeError(f"No validation rows for holdout '{holdout_station}'")
    if y_train.sum() == 0:
        raise RuntimeError("No positive training examples; rain never observed in training set")

    return X_train, y_train, X_val, y_val, base_val, present_features, stats


def _compute_metrics(
    y_val: np.ndarray,
    y_proba: np.ndarray,
    base_val_precip_mm: np.ndarray,
    thresholds: tuple[float, ...] = THRESHOLDS,
) -> tuple[dict, dict]:
    baseline_pred = (base_val_precip_mm > BASELINE_PRECIP_THRESHOLD_MM).astype(int)
    baseline_precision = float(precision_score(y_val, baseline_pred, zero_division=0))
    baseline_recall = float(recall_score(y_val, baseline_pred, zero_division=0))
    baseline_f1 = float(f1_score(y_val, baseline_pred, zero_division=0))
    baseline_accuracy = float(np.mean(baseline_pred == y_val))

    try:
        roc_auc = float(roc_auc_score(y_val, y_proba))
    except ValueError:
        roc_auc = float("nan")
    try:
        avg_prec = float(average_precision_score(y_val, y_proba))
    except ValueError:
        avg_prec = float("nan")
    brier = float(brier_score_loss(y_val, y_proba))

    metrics_at_threshold = {}
    for t in thresholds:
        pred = (y_proba >= t).astype(int)
        metrics_at_threshold[str(t)] = {
            "precision": float(precision_score(y_val, pred, zero_division=0)),
            "recall": float(recall_score(y_val, pred, zero_division=0)),
            "f1": float(f1_score(y_val, pred, zero_division=0)),
            "accuracy": float(np.mean(pred == y_val)),
            "n_positive_predictions": int(pred.sum()),
        }

    baseline = {
        "precision": baseline_precision,
        "recall": baseline_recall,
        "f1": baseline_f1,
        "accuracy": baseline_accuracy,
        "n_positive_predictions": int(baseline_pred.sum()),
    }
    threshold_free = {
        "roc_auc": roc_auc,
        "average_precision": avg_prec,
        "brier_score": brier,
    }
    return baseline, {**threshold_free, "metrics_at_threshold": metrics_at_threshold}


def fit_logreg_rain(
    framed: pd.DataFrame,
    lead: int,
    holdout_station: str,
    *,
    config: LogRegConfig,
) -> tuple[RainClassifierResult, dict]:
    X_train, y_train, X_val, y_val, base_val, present_features, stats = _prepare_xy(
        framed, lead, holdout_station
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)

    model = LogisticRegression(
        C=config.C,
        max_iter=config.max_iter,
        class_weight=config.class_weight,
        random_state=config.random_state,
    )
    model.fit(X_train_scaled, y_train)
    y_proba = model.predict_proba(X_val_scaled)[:, 1]

    baseline, model_metrics = _compute_metrics(y_val, y_proba, base_val)
    f1_at_03 = model_metrics["metrics_at_threshold"]["0.3"]["f1"]
    f1_improvement = f1_at_03 - baseline["f1"]

    result = RainClassifierResult(
        target="rain_occurrence",
        lead=lead,
        holdout_station=holdout_station,
        model_class="LogisticRegression",
        hp_label=config.label(),
        hp=config.as_dict(),
        n_train=int(len(X_train)),
        n_val=int(len(X_val)),
        positive_rate_train=float(np.mean(y_train)),
        positive_rate_val=float(np.mean(y_val)),
        feature_columns=present_features,
        baseline_precision=baseline["precision"],
        baseline_recall=baseline["recall"],
        baseline_f1=baseline["f1"],
        baseline_accuracy=baseline["accuracy"],
        roc_auc=model_metrics["roc_auc"],
        average_precision=model_metrics["average_precision"],
        brier_score=model_metrics["brier_score"],
        metrics_at_threshold=model_metrics["metrics_at_threshold"],
        f1_at_03=f1_at_03,
        f1_improvement_at_03=f1_improvement,
    )

    artifact = {
        "model": model,
        "scaler": scaler,
        "imputation_stats": stats,
        "feature_columns": present_features,
        "target": "rain_occurrence",
        "lead": lead,
        "predicts_residual": False,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "holdout_station": holdout_station,
        "config": config.as_dict(),
        "config_label": config.label(),
        "model_class": "LogisticRegression",
        "output_type": "probability",
    }

    return result, artifact


def fit_hgb_classifier_rain(
    framed: pd.DataFrame,
    lead: int,
    holdout_station: str,
    *,
    config: HGBClassifierConfig,
) -> tuple[RainClassifierResult, dict]:
    X_train, y_train, X_val, y_val, base_val, present_features, stats = _prepare_xy(
        framed, lead, holdout_station
    )

    model = HistGradientBoostingClassifier(
        max_depth=config.max_depth,
        learning_rate=config.learning_rate,
        max_iter=config.max_iter,
        early_stopping=config.early_stopping,
        validation_fraction=config.validation_fraction,
        n_iter_no_change=config.n_iter_no_change,
        class_weight=config.class_weight,
        random_state=config.random_state,
    )
    model.fit(X_train, y_train)
    y_proba = model.predict_proba(X_val)[:, 1]
    n_iter_used = int(getattr(model, "n_iter_", config.max_iter))

    baseline, model_metrics = _compute_metrics(y_val, y_proba, base_val)
    f1_at_03 = model_metrics["metrics_at_threshold"]["0.3"]["f1"]
    f1_improvement = f1_at_03 - baseline["f1"]

    result = RainClassifierResult(
        target="rain_occurrence",
        lead=lead,
        holdout_station=holdout_station,
        model_class="HistGradientBoostingClassifier",
        hp_label=config.label(),
        hp={**config.as_dict(), "n_iter_used": n_iter_used},
        n_train=int(len(X_train)),
        n_val=int(len(X_val)),
        positive_rate_train=float(np.mean(y_train)),
        positive_rate_val=float(np.mean(y_val)),
        feature_columns=present_features,
        baseline_precision=baseline["precision"],
        baseline_recall=baseline["recall"],
        baseline_f1=baseline["f1"],
        baseline_accuracy=baseline["accuracy"],
        roc_auc=model_metrics["roc_auc"],
        average_precision=model_metrics["average_precision"],
        brier_score=model_metrics["brier_score"],
        metrics_at_threshold=model_metrics["metrics_at_threshold"],
        f1_at_03=f1_at_03,
        f1_improvement_at_03=f1_improvement,
    )

    artifact = {
        "model": model,
        "scaler": None,
        "imputation_stats": stats,
        "feature_columns": present_features,
        "target": "rain_occurrence",
        "lead": lead,
        "predicts_residual": False,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "holdout_station": holdout_station,
        "config": config.as_dict(),
        "config_label": config.label(),
        "n_iter_used": n_iter_used,
        "model_class": "HistGradientBoostingClassifier",
        "output_type": "probability",
    }

    return result, artifact


def save_artifact(artifact: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, path)
