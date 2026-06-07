from __future__ import annotations

import json
import time

import numpy as np
from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    HistGradientBoostingRegressor,
)
from sklearn.linear_model import Ridge
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.preprocessing import StandardScaler

from postprocessing.training.data_loader import (
    load_canonical,
    target_columns_for,
    within_station_hourly_split,
)
from postprocessing.training.feature_selection import features_for
from postprocessing.training.imputation import (
    apply_imputation,
    compute_imputation_stats,
)
from postprocessing.training.preparation import prepare_for_target
from postprocessing.training.rain_classifier_runner import (
    BASELINE_PRECIP_COLUMN,
    BASELINE_PRECIP_THRESHOLD_MM,
)
from postprocessing.training.ridge_runner import BASELINE_COLUMN
from postprocessing.utils.paths import get_paths


LEADS: tuple[int, ...] = (1, 6, 24, 48, 72)
WITHIN_STATION_FRACTION: float = 0.10
WITHIN_STATION_BLOCK_HOURS: int = 48
WITHIN_STATION_SEED: int = 42
POSITIVE_RAIN_THRESHOLD_MM: float = 0.1


WINNING_CONFIG = {
    "temperature": {"model": "HGB", "hp": {"max_depth": 10, "learning_rate": 0.1}},
    "relative_humidity": {"model": "HGB", "hp": {"max_depth": 10, "learning_rate": 0.1}},
    "dew_point": {"model": "HGB", "hp": {"max_depth": 10, "learning_rate": 0.1}},
    "wind_speed": {"model": "Ridge", "hp": {"alpha": 10.0}},
    "wind_gust": {"model": "Ridge", "hp": {"alpha": 0.1}},
    "pressure": {"model": "Ridge", "hp": {"alpha": 0.1}},
    "uv": {"model": "HGB", "hp": {"max_depth": 10, "learning_rate": 0.1}},
    "wind_direction": {"model": "Ridge", "hp": {"alpha": 10.0}},
    "rain_occurrence": {"model": "HGB", "hp": {"max_depth": 10, "learning_rate": 0.1, "class_weight": "balanced"}},
    "rain_amount": {"model": "HGB", "hp": {"max_depth": 10, "learning_rate": 0.1}},
}


def _make_hgb_regressor(hp):
    return HistGradientBoostingRegressor(
        max_depth=hp["max_depth"],
        learning_rate=hp["learning_rate"],
        max_iter=500,
        early_stopping=True,
        validation_fraction=0.15,
        n_iter_no_change=25,
        random_state=42,
    )


def _make_hgb_classifier(hp):
    return HistGradientBoostingClassifier(
        max_depth=hp["max_depth"],
        learning_rate=hp["learning_rate"],
        max_iter=500,
        early_stopping=True,
        validation_fraction=0.15,
        n_iter_no_change=25,
        class_weight=hp.get("class_weight"),
        random_state=42,
    )


def _residual_reduction(baseline_mae, corrected_mae):
    return 100.0 * (baseline_mae - corrected_mae) / baseline_mae if baseline_mae > 0 else 0.0


def _evaluate_residual_target(framed, target, lead, hp, model_class):
    target_col = f"{target_columns_for(target)[0]}_lead_{lead}h"
    baseline_col = BASELINE_COLUMN[target]
    baseline_lead_col = f"{baseline_col}_lead_{lead}h"

    feature_list = features_for(target, lead)
    present_features = [c for c in feature_list if c in framed.columns]

    train_idx, val_idx = within_station_hourly_split(
        framed,
        fraction=WITHIN_STATION_FRACTION,
        block_hours=WITHIN_STATION_BLOCK_HOURS,
        seed=WITHIN_STATION_SEED,
    )
    train_df = framed.iloc[train_idx].copy()
    val_df = framed.iloc[val_idx].copy()

    stats = compute_imputation_stats(train_df, present_features)
    train_df = apply_imputation(train_df, stats)
    val_df = apply_imputation(val_df, stats)

    X_train = train_df[present_features].to_numpy(dtype=float)
    X_val = val_df[present_features].to_numpy(dtype=float)
    y_train = train_df[target_col].to_numpy(dtype=float)
    y_val = val_df[target_col].to_numpy(dtype=float)

    predicts_residual = target in {"temperature", "relative_humidity", "dew_point", "wind_speed", "wind_gust", "pressure"}

    if predicts_residual:
        baseline_pred = np.zeros_like(y_val)
        keep_train = ~np.any(np.isnan(X_train), axis=1) & ~np.isnan(y_train)
        keep_val = ~np.any(np.isnan(X_val), axis=1) & ~np.isnan(y_val)
    else:
        if baseline_lead_col not in val_df.columns:
            raise RuntimeError(f"Missing baseline column {baseline_lead_col}")
        baseline_pred_full = val_df[baseline_lead_col].to_numpy(dtype=float)
        baseline_pred_train = train_df[baseline_lead_col].to_numpy(dtype=float)
        keep_train = ~np.any(np.isnan(X_train), axis=1) & ~np.isnan(y_train) & ~np.isnan(baseline_pred_train)
        keep_val = ~np.any(np.isnan(X_val), axis=1) & ~np.isnan(y_val) & ~np.isnan(baseline_pred_full)
        baseline_pred = baseline_pred_full

    X_train, y_train = X_train[keep_train], y_train[keep_train]
    X_val, y_val = X_val[keep_val], y_val[keep_val]
    baseline_pred = baseline_pred[keep_val]

    if model_class == "Ridge":
        scaler = StandardScaler()
        X_train_s = scaler.fit_transform(X_train)
        X_val_s = scaler.transform(X_val)
        model = Ridge(alpha=hp["alpha"])
        model.fit(X_train_s, y_train)
        y_pred = model.predict(X_val_s)
    else:
        model = _make_hgb_regressor(hp)
        model.fit(X_train, y_train)
        y_pred = model.predict(X_val)

    baseline_errors = y_val - baseline_pred
    corrected_errors = y_val - y_pred

    baseline_mae = float(np.mean(np.abs(baseline_errors)))
    corrected_mae = float(np.mean(np.abs(corrected_errors)))
    baseline_rmse = float(np.sqrt(np.mean(baseline_errors ** 2)))
    corrected_rmse = float(np.sqrt(np.mean(corrected_errors ** 2)))
    baseline_bias = float(np.mean(baseline_errors))
    corrected_bias = float(np.mean(corrected_errors))

    return {
        "metric_type": "residual_or_absolute",
        "n_train": int(len(X_train)),
        "n_val": int(len(X_val)),
        "baseline_mae": baseline_mae,
        "baseline_rmse": baseline_rmse,
        "baseline_bias": baseline_bias,
        "corrected_mae": corrected_mae,
        "corrected_rmse": corrected_rmse,
        "corrected_bias": corrected_bias,
        "mae_reduction_pct": _residual_reduction(baseline_mae, corrected_mae),
        "rmse_reduction_pct": _residual_reduction(baseline_rmse, corrected_rmse),
    }


def _evaluate_wind_direction(framed, lead, hp, model_class):
    sin_col = f"winddir_residual_sin_lead_{lead}h"
    cos_col = f"winddir_residual_cos_lead_{lead}h"
    base_sin_col = f"base_wind_direction_sin_lead_{lead}h"
    base_cos_col = f"base_wind_direction_cos_lead_{lead}h"

    feature_list = features_for("wind_direction", lead)
    present_features = [c for c in feature_list if c in framed.columns]

    train_idx, val_idx = within_station_hourly_split(
        framed,
        fraction=WITHIN_STATION_FRACTION,
        block_hours=WITHIN_STATION_BLOCK_HOURS,
        seed=WITHIN_STATION_SEED,
    )
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
    base_sin = val_df[base_sin_col].to_numpy(dtype=float)
    base_cos = val_df[base_cos_col].to_numpy(dtype=float)

    keep_train = (
        ~np.any(np.isnan(X_train), axis=1)
        & ~np.isnan(y_train_sin) & ~np.isnan(y_train_cos)
    )
    keep_val = (
        ~np.any(np.isnan(X_val), axis=1)
        & ~np.isnan(y_val_sin) & ~np.isnan(y_val_cos)
        & ~np.isnan(base_sin) & ~np.isnan(base_cos)
    )
    X_train = X_train[keep_train]
    y_train = np.column_stack([y_train_sin[keep_train], y_train_cos[keep_train]])
    X_val = X_val[keep_val]
    y_val_sin = y_val_sin[keep_val]
    y_val_cos = y_val_cos[keep_val]
    base_sin = base_sin[keep_val]
    base_cos = base_cos[keep_val]

    if model_class == "Ridge":
        scaler = StandardScaler()
        X_train_s = scaler.fit_transform(X_train)
        X_val_s = scaler.transform(X_val)
        model = Ridge(alpha=hp["alpha"])
        model.fit(X_train_s, y_train)
        y_pred = model.predict(X_val_s)
        pred_sin = y_pred[:, 0]
        pred_cos = y_pred[:, 1]
    else:
        model_sin = _make_hgb_regressor(hp)
        model_sin.fit(X_train, y_train[:, 0])
        model_cos = _make_hgb_regressor(hp)
        model_cos.fit(X_train, y_train[:, 1])
        pred_sin = model_sin.predict(X_val)
        pred_cos = model_cos.predict(X_val)

    actual_angle = np.mod(np.degrees(np.arctan2(y_val_sin, y_val_cos)), 360.0)
    base_angle = np.mod(np.degrees(np.arctan2(base_sin, base_cos)), 360.0)
    corr_sin = base_sin + pred_sin
    corr_cos = base_cos + pred_cos
    norm = np.sqrt(corr_sin ** 2 + corr_cos ** 2)
    norm = np.where(norm < 1e-9, 1.0, norm)
    corr_angle = np.mod(np.degrees(np.arctan2(corr_sin / norm, corr_cos / norm)), 360.0)

    def circular_mae(p, a):
        d = np.mod(p - a + 180.0, 360.0) - 180.0
        return float(np.mean(np.abs(d)))

    baseline_mae = circular_mae(base_angle, actual_angle)
    corrected_mae = circular_mae(corr_angle, actual_angle)

    return {
        "metric_type": "circular_mae_deg",
        "n_train": int(len(X_train)),
        "n_val": int(len(X_val)),
        "baseline_mae_deg": baseline_mae,
        "corrected_mae_deg": corrected_mae,
        "mae_reduction_pct": _residual_reduction(baseline_mae, corrected_mae),
    }


def _evaluate_rain_occurrence(framed, lead, hp):
    target_col = f"rain_occurrence_lead_{lead}h"
    baseline_col = f"{BASELINE_PRECIP_COLUMN}_lead_{lead}h"

    feature_list = features_for("rain_occurrence", lead)
    present_features = [c for c in feature_list if c in framed.columns]

    train_idx, val_idx = within_station_hourly_split(
        framed,
        fraction=WITHIN_STATION_FRACTION,
        block_hours=WITHIN_STATION_BLOCK_HOURS,
        seed=WITHIN_STATION_SEED,
    )
    train_df = framed.iloc[train_idx].copy()
    val_df = framed.iloc[val_idx].copy()

    stats = compute_imputation_stats(train_df, present_features)
    train_df = apply_imputation(train_df, stats)
    val_df = apply_imputation(val_df, stats)

    X_train = train_df[present_features].to_numpy(dtype=float)
    X_val = val_df[present_features].to_numpy(dtype=float)
    y_train = train_df[target_col].to_numpy(dtype=float)
    y_val = val_df[target_col].to_numpy(dtype=float)
    base_val = val_df[baseline_col].to_numpy(dtype=float)
    base_train = train_df[baseline_col].to_numpy(dtype=float)

    keep_train = ~np.any(np.isnan(X_train), axis=1) & ~np.isnan(y_train) & ~np.isnan(base_train)
    keep_val = ~np.any(np.isnan(X_val), axis=1) & ~np.isnan(y_val) & ~np.isnan(base_val)
    X_train, y_train = X_train[keep_train], y_train[keep_train].astype(int)
    X_val, y_val = X_val[keep_val], y_val[keep_val].astype(int)
    base_val = base_val[keep_val]

    model = _make_hgb_classifier(hp)
    model.fit(X_train, y_train)
    y_proba = model.predict_proba(X_val)[:, 1]
    y_pred_03 = (y_proba >= 0.3).astype(int)
    baseline_pred = (base_val > BASELINE_PRECIP_THRESHOLD_MM).astype(int)

    return {
        "metric_type": "classification_f1",
        "n_train": int(len(X_train)),
        "n_val": int(len(X_val)),
        "positive_rate_val": float(np.mean(y_val)),
        "roc_auc": float(roc_auc_score(y_val, y_proba)),
        "average_precision": float(average_precision_score(y_val, y_proba)),
        "brier_score": float(brier_score_loss(y_val, y_proba)),
        "baseline_f1": float(f1_score(y_val, baseline_pred, zero_division=0)),
        "model_f1_at_03": float(f1_score(y_val, y_pred_03, zero_division=0)),
        "model_precision_at_03": float(precision_score(y_val, y_pred_03, zero_division=0)),
        "model_recall_at_03": float(recall_score(y_val, y_pred_03, zero_division=0)),
    }


def _evaluate_rain_amount(framed, lead, hp):
    log1p_col = f"rain_amount_log1p_lead_{lead}h"
    baseline_col = f"{BASELINE_PRECIP_COLUMN}_lead_{lead}h"

    feature_list = features_for("rain_amount", lead)
    present_features = [c for c in feature_list if c in framed.columns]

    train_idx, val_idx = within_station_hourly_split(
        framed,
        fraction=WITHIN_STATION_FRACTION,
        block_hours=WITHIN_STATION_BLOCK_HOURS,
        seed=WITHIN_STATION_SEED,
    )
    train_df = framed.iloc[train_idx].copy()
    val_df = framed.iloc[val_idx].copy()

    train_mm = np.expm1(train_df[log1p_col].to_numpy(dtype=float))
    val_mm = np.expm1(val_df[log1p_col].to_numpy(dtype=float))
    train_pos = train_df[train_mm >= POSITIVE_RAIN_THRESHOLD_MM].copy()
    val_pos = val_df[val_mm >= POSITIVE_RAIN_THRESHOLD_MM].copy()

    if len(train_pos) < 50 or len(val_pos) == 0:
        raise RuntimeError(f"Insufficient positive rows: train={len(train_pos)}, val={len(val_pos)}")

    stats = compute_imputation_stats(train_pos, present_features)
    train_pos = apply_imputation(train_pos, stats)
    val_pos = apply_imputation(val_pos, stats)

    X_train = train_pos[present_features].to_numpy(dtype=float)
    X_val = val_pos[present_features].to_numpy(dtype=float)
    y_train_log1p = train_pos[log1p_col].to_numpy(dtype=float)
    y_val_log1p = val_pos[log1p_col].to_numpy(dtype=float)
    y_val_mm = np.expm1(y_val_log1p)
    base_val_mm = val_pos[baseline_col].to_numpy(dtype=float)

    keep_train = ~np.any(np.isnan(X_train), axis=1) & ~np.isnan(y_train_log1p)
    keep_val = ~np.any(np.isnan(X_val), axis=1) & ~np.isnan(y_val_mm) & ~np.isnan(base_val_mm)
    X_train, y_train_log1p = X_train[keep_train], y_train_log1p[keep_train]
    X_val, y_val_mm = X_val[keep_val], y_val_mm[keep_val]
    base_val_mm = base_val_mm[keep_val]

    model = _make_hgb_regressor(hp)
    model.fit(X_train, y_train_log1p)
    pred_log1p = model.predict(X_val)
    pred_mm = np.clip(np.expm1(pred_log1p), 0.0, None)

    baseline_mae = float(np.mean(np.abs(y_val_mm - base_val_mm)))
    corrected_mae = float(np.mean(np.abs(y_val_mm - pred_mm)))

    return {
        "metric_type": "rain_amount_conditional_mm",
        "n_train": int(len(X_train)),
        "n_val_positive": int(len(X_val)),
        "baseline_mae_mm": baseline_mae,
        "corrected_mae_mm": corrected_mae,
        "mae_reduction_pct": _residual_reduction(baseline_mae, corrected_mae),
        "actual_mean_mm": float(np.mean(y_val_mm)),
    }


def evaluate_target(canonical, target, lead):
    config = WINNING_CONFIG[target]
    model_class = config["model"]
    hp = config["hp"]
    framed = prepare_for_target(canonical, target, lead)

    if target == "wind_direction":
        result = _evaluate_wind_direction(framed, lead, hp, model_class)
    elif target == "rain_occurrence":
        result = _evaluate_rain_occurrence(framed, lead, hp)
    elif target == "rain_amount":
        result = _evaluate_rain_amount(framed, lead, hp)
    else:
        result = _evaluate_residual_target(framed, target, lead, hp, model_class)

    result["target"] = target
    result["lead"] = lead
    result["model_class"] = model_class
    result["hp"] = hp
    return result


def load_loso_summary(target, lead, model_class):
    paths = get_paths()
    if target == "rain_occurrence":
        path = paths.reports.diagnostics_dir / "rain_hgb_classifier_sweep" / f"rain_occurrence_lead{lead}.json"
    elif target == "rain_amount":
        path = paths.reports.diagnostics_dir / "rain_amount_hgb_sweep" / f"rain_amount_lead{lead}.json"
    else:
        sub = "ridge_loso_sweep" if model_class == "Ridge" else "hgb_loso_sweep"
        path = paths.reports.diagnostics_dir / sub / f"{target}_lead{lead}.json"
    if not path.exists():
        return None
    with path.open() as f:
        data = json.load(f)
    summary = data.get("summary", {})
    return summary.get("best_summary")


def loso_metric_for_comparison(target, loso_best):
    if loso_best is None:
        return None
    if target == "uv":
        return ("abs_mae", loso_best.get("network_mean_absolute_mae"))
    if target == "wind_direction":
        return ("mae_red_pct", loso_best.get("network_mean_mae_reduction_pct"))
    if target == "rain_occurrence":
        return ("f1_at_0.3", loso_best.get("network_mean_f1_at_03"))
    if target == "rain_amount":
        return ("mae_red_pct", loso_best.get("network_mean_mae_reduction_pct"))
    return ("mae_red_pct", loso_best.get("network_mean_mae_reduction_pct"))


def within_station_metric_for_comparison(target, ws_result):
    if target == "uv":
        return ("abs_mae", ws_result["corrected_mae"])
    if target == "wind_direction":
        return ("mae_red_pct", ws_result["mae_reduction_pct"])
    if target == "rain_occurrence":
        return ("f1_at_0.3", ws_result["model_f1_at_03"])
    if target == "rain_amount":
        return ("mae_red_pct", ws_result["mae_reduction_pct"])
    return ("mae_red_pct", ws_result["mae_reduction_pct"])


def main():
    paths = get_paths()
    print("Loading canonical")
    df = load_canonical()
    print(f"  Loaded: {len(df):,} rows, {df['station_id'].nunique()} stations")
    print()
    print(f"Within-station validation: fraction={WITHIN_STATION_FRACTION}, block_hours={WITHIN_STATION_BLOCK_HOURS}, seed={WITHIN_STATION_SEED}")
    print()

    output_dir = paths.reports.diagnostics_dir / "within_station_validation"
    output_dir.mkdir(parents=True, exist_ok=True)

    all_results = {}
    total_start = time.time()

    for target in WINNING_CONFIG:
        print(f"--- {target} ({WINNING_CONFIG[target]['model']}) ---")
        results_by_lead = {}
        for lead in LEADS:
            t0 = time.time()
            try:
                result = evaluate_target(df, target, lead)
                results_by_lead[lead] = result
                elapsed = time.time() - t0

                if target == "rain_occurrence":
                    print(
                        f"  lead={lead:>2}h  n_val={result['n_val']:>5,}  "
                        f"pos={result['positive_rate_val']*100:>4.1f}%  "
                        f"roc_auc={result['roc_auc']:.3f}  "
                        f"base_f1={result['baseline_f1']:.3f}  "
                        f"f1@0.3={result['model_f1_at_03']:.3f}  "
                        f"(p={result['model_precision_at_03']:.3f}, r={result['model_recall_at_03']:.3f})  "
                        f"time={elapsed:.1f}s"
                    )
                elif target == "rain_amount":
                    print(
                        f"  lead={lead:>2}h  n_val_pos={result['n_val_positive']:>4}  "
                        f"actual_mean={result['actual_mean_mm']:.2f}mm  "
                        f"base_mae={result['baseline_mae_mm']:.2f}  "
                        f"corr_mae={result['corrected_mae_mm']:.2f}  "
                        f"red={result['mae_reduction_pct']:+.2f}%  "
                        f"time={elapsed:.1f}s"
                    )
                elif target == "wind_direction":
                    print(
                        f"  lead={lead:>2}h  n_val={result['n_val']:>5,}  "
                        f"base_mae={result['baseline_mae_deg']:.2f}deg  "
                        f"corr_mae={result['corrected_mae_deg']:.2f}deg  "
                        f"red={result['mae_reduction_pct']:+.2f}%  "
                        f"time={elapsed:.1f}s"
                    )
                elif target == "uv":
                    print(
                        f"  lead={lead:>2}h  n_val={result['n_val']:>5,}  "
                        f"corr_mae={result['corrected_mae']:.3f}  "
                        f"corr_rmse={result['corrected_rmse']:.3f}  "
                        f"corr_bias={result['corrected_bias']:+.3f}  "
                        f"time={elapsed:.1f}s"
                    )
                else:
                    print(
                        f"  lead={lead:>2}h  n_val={result['n_val']:>5,}  "
                        f"base_mae={result['baseline_mae']:.3f}  "
                        f"corr_mae={result['corrected_mae']:.3f}  "
                        f"red={result['mae_reduction_pct']:+.2f}%  "
                        f"base_bias={result['baseline_bias']:+.3f}  "
                        f"corr_bias={result['corrected_bias']:+.3f}  "
                        f"time={elapsed:.1f}s"
                    )
            except Exception as exc:
                results_by_lead[lead] = {"error": str(exc), "lead": lead}
                print(f"  lead={lead}h  ERROR: {exc}")

        all_results[target] = results_by_lead
        path = output_dir / f"{target}_within_station.json"
        with path.open("w") as f:
            json.dump({
                "target": target,
                "model_class": WINNING_CONFIG[target]["model"],
                "hp": WINNING_CONFIG[target]["hp"],
                "split_config": {
                    "fraction": WITHIN_STATION_FRACTION,
                    "block_hours": WITHIN_STATION_BLOCK_HOURS,
                    "seed": WITHIN_STATION_SEED,
                },
                "results_by_lead": {str(k): v for k, v in results_by_lead.items()},
            }, f, indent=2)
        print()

    print(f"=== Total time: {time.time() - total_start:.1f}s ===")
    print()

    print("=== LOSO vs Within-station comparison ===")
    print(f"  {'target':>20}  {'model':>6}  {'metric':>11}  {'L1 LOSO':>10}  {'L1 WS':>10}  {'L24 LOSO':>10}  {'L24 WS':>10}  {'L72 LOSO':>10}  {'L72 WS':>10}")
    comparison = {}
    for target in WINNING_CONFIG:
        model_class = WINNING_CONFIG[target]["model"]
        row = [f"  {target:>20}", f"{model_class:>6}"]
        comparison[target] = {"model": model_class, "leads": {}}
        metric_label = None
        for lead in (1, 24, 72):
            loso_best = load_loso_summary(target, lead, model_class)
            loso_pair = loso_metric_for_comparison(target, loso_best)
            ws_result = all_results[target].get(lead)
            if ws_result is None or "error" in ws_result:
                loso_str = f"{'---':>10}"
                ws_str = f"{'---':>10}"
                row.extend([loso_str, ws_str])
                continue
            ws_pair = within_station_metric_for_comparison(target, ws_result)
            if loso_pair is None or loso_pair[1] is None:
                loso_str = f"{'---':>10}"
            else:
                metric_label = loso_pair[0]
                if metric_label == "mae_red_pct":
                    loso_str = f"{loso_pair[1]:>+9.2f}%"
                else:
                    loso_str = f"{loso_pair[1]:>10.3f}"
            metric_label = ws_pair[0]
            if metric_label == "mae_red_pct":
                ws_str = f"{ws_pair[1]:>+9.2f}%"
            else:
                ws_str = f"{ws_pair[1]:>10.3f}"
            row.extend([loso_str, ws_str])
            comparison[target]["leads"][lead] = {
                "loso": loso_pair[1] if loso_pair else None,
                "within_station": ws_pair[1],
                "metric": metric_label,
            }
        if metric_label is None:
            metric_label = "---"
        row.insert(2, f"{metric_label:>11}")
        print("  ".join(row))
    print()

    aggregate_path = output_dir / "loso_vs_within_station_comparison.json"
    with aggregate_path.open("w") as f:
        json.dump(comparison, f, indent=2)
    print(f"Comparison saved: {aggregate_path}")


if __name__ == "__main__":
    main()
