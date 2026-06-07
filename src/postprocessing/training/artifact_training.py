from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

from postprocessing.training.data_loader import target_columns_for
from postprocessing.training.feature_selection import features_for
from postprocessing.training.imputation import apply_imputation, compute_imputation_stats
from postprocessing.training.preparation import prepare_for_target
from postprocessing.training.ridge_runner import BASELINE_COLUMN


ARTIFACT_DIR = Path("models/artifacts")


def predicts_residual(target):
    return target in ("temperature", "relative_humidity", "dew_point",
                      "wind_speed", "wind_gust", "pressure")


def make_hgb(hp):
    return HistGradientBoostingRegressor(
        max_depth=hp["max_depth"],
        learning_rate=hp["learning_rate"],
        max_iter=500,
        early_stopping=True,
        validation_fraction=0.15,
        n_iter_no_change=25,
        random_state=42,
    )


def make_ridge(hp):
    return Ridge(alpha=hp["alpha"])


def train_scalar_target_artifact(canonical, target, lead, model_class, hp):
    framed = prepare_for_target(canonical, target, lead)
    target_col_base = target_columns_for(target)[0]
    target_col = f"{target_col_base}_lead_{lead}h"

    feature_list = features_for(target, lead)
    present_features = [c for c in feature_list if c in framed.columns]

    stats = compute_imputation_stats(framed, present_features)
    framed_imp = apply_imputation(framed, stats)

    X = framed_imp[present_features].to_numpy(dtype=float)
    y = framed_imp[target_col].to_numpy(dtype=float)
    keep = ~np.any(np.isnan(X), axis=1) & ~np.isnan(y)
    X, y = X[keep], y[keep]

    if len(X) == 0:
        raise RuntimeError(f"No training rows after NaN filter for {target} lead={lead}")

    if model_class == "Ridge":
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        model = make_ridge(hp)
        model.fit(X_scaled, y)
    else:
        scaler = None
        model = make_hgb(hp)
        model.fit(X, y)

    artifact = {
        "model": model,
        "scaler": scaler,
        "imputation_stats": stats,
        "feature_columns": present_features,
        "target": target,
        "lead": lead,
        "model_class": model_class,
        "hp": hp,
        "n_train": int(len(X)),
        "predicts_residual": predicts_residual(target),
        "baseline_column": BASELINE_COLUMN.get(target),
    }
    return artifact


def train_wind_direction_artifact(canonical, lead, hp):
    framed = prepare_for_target(canonical, "wind_direction", lead)
    sin_col = f"winddir_residual_sin_lead_{lead}h"
    cos_col = f"winddir_residual_cos_lead_{lead}h"

    feature_list = features_for("wind_direction", lead)
    present_features = [c for c in feature_list if c in framed.columns]

    stats = compute_imputation_stats(framed, present_features)
    framed_imp = apply_imputation(framed, stats)

    X = framed_imp[present_features].to_numpy(dtype=float)
    y_sin = framed_imp[sin_col].to_numpy(dtype=float)
    y_cos = framed_imp[cos_col].to_numpy(dtype=float)
    keep = ~np.any(np.isnan(X), axis=1) & ~np.isnan(y_sin) & ~np.isnan(y_cos)
    X = X[keep]
    y = np.column_stack([y_sin[keep], y_cos[keep]])

    if len(X) == 0:
        raise RuntimeError(f"No training rows after NaN filter for wind_direction lead={lead}")

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    model = make_ridge(hp)
    model.fit(X_scaled, y)

    artifact = {
        "model": model,
        "scaler": scaler,
        "imputation_stats": stats,
        "feature_columns": present_features,
        "target": "wind_direction",
        "lead": lead,
        "model_class": "Ridge",
        "hp": hp,
        "n_train": int(len(X)),
        "predicts_residual": True,
        "output_layout": ["winddir_residual_sin", "winddir_residual_cos"],
        "reconstruction": "angle_convention",
    }
    return artifact


def artifact_path(target, lead, model_class):
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    return ARTIFACT_DIR / f"{target}_{model_class}_lead{lead}h.joblib"


def save_artifact(artifact, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, path, compress=3)


def load_artifact(path):
    return joblib.load(path)


def predict_with_artifact(artifact, df):
    features = artifact["feature_columns"]
    stats = artifact["imputation_stats"]
    df_imp = apply_imputation(df, stats)
    X = df_imp.reindex(columns=features).to_numpy(dtype=float)
    keep = ~np.any(np.isnan(X), axis=1)
    if keep.sum() == 0:
        return np.array([]), np.array([], dtype=bool)
    X = X[keep]
    if artifact["scaler"] is not None:
        X = artifact["scaler"].transform(X)
    pred = artifact["model"].predict(X)
    return pred, keep
