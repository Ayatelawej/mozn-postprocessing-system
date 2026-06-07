from __future__ import annotations

import json
import time

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

from postprocessing.training.data_loader import load_canonical, loso_split
from postprocessing.training.feature_selection import features_for
from postprocessing.training.imputation import apply_imputation, compute_imputation_stats
from postprocessing.training.preparation import prepare_for_target
from postprocessing.training.rain_classifier_runner import (
    BASELINE_PRECIP_COLUMN,
    BASELINE_PRECIP_THRESHOLD_MM,
)
from postprocessing.utils.paths import get_paths


LEADS: tuple[int, ...] = (1, 6, 24, 48, 72)
POSITIVE_RAIN_THRESHOLD_MM: float = 0.1
OVERRIDE_STATIONS: set[str] = {"IMURQU7"}


PERSISTENCE_FEATURE_PATTERNS: tuple[str, ...] = (
    "station_rain_event",
    "station_rain_rolling_",
    "rain_total_mm_lag_",
    "rain_total_mm_roll_",
    "relative_humidity_pct_lag_",
    "pressure_max_hpa_lag_",
)


def is_persistence_feature(name: str) -> bool:
    return any(p in name for p in PERSISTENCE_FEATURE_PATTERNS)


def experiment_a_per_lead(canonical, lead):
    framed = prepare_for_target(canonical, "rain_occurrence", lead)
    target_col = f"rain_occurrence_lead_{lead}h"
    persistence_col = "station_rain_event"

    stations = sorted(framed["station_id"].unique())
    stations = [s for s in stations if s not in OVERRIDE_STATIONS]

    per_station = []
    for sid in stations:
        station_rows = framed[framed["station_id"] == sid]
        actual = station_rows[target_col].to_numpy(dtype=float)
        persistence_pred = station_rows[persistence_col].to_numpy(dtype=float)
        keep = ~np.isnan(actual) & ~np.isnan(persistence_pred)
        if keep.sum() == 0:
            continue
        actual_clean = actual[keep].astype(int)
        persistence_clean = persistence_pred[keep].astype(int)
        if actual_clean.sum() == 0:
            continue
        per_station.append({
            "station_id": sid,
            "n_rows": int(keep.sum()),
            "positive_rate": float(np.mean(actual_clean)),
            "persistence_f1": float(f1_score(actual_clean, persistence_clean, zero_division=0)),
            "persistence_precision": float(precision_score(actual_clean, persistence_clean, zero_division=0)),
            "persistence_recall": float(recall_score(actual_clean, persistence_clean, zero_division=0)),
            "persistence_accuracy": float(np.mean(actual_clean == persistence_clean)),
        })
    if not per_station:
        return None
    n = len(per_station)
    return {
        "lead": lead,
        "n_stations": n,
        "mean_positive_rate": sum(r["positive_rate"] for r in per_station) / n,
        "mean_persistence_f1": sum(r["persistence_f1"] for r in per_station) / n,
        "mean_persistence_precision": sum(r["persistence_precision"] for r in per_station) / n,
        "mean_persistence_recall": sum(r["persistence_recall"] for r in per_station) / n,
        "mean_persistence_accuracy": sum(r["persistence_accuracy"] for r in per_station) / n,
    }


def load_rain_occurrence_hgb_summary(lead):
    paths = get_paths()
    path = paths.reports.diagnostics_dir / "rain_hgb_classifier_sweep" / f"rain_occurrence_lead{lead}.json"
    if not path.exists():
        return None
    with path.open() as f:
        data = json.load(f)
    best = data.get("summary", {}).get("best_summary", {})
    return {
        "f1_at_03": best.get("network_mean_f1_at_03"),
        "precision_at_03": best.get("network_mean_precision_at_03"),
        "recall_at_03": best.get("network_mean_recall_at_03"),
        "roc_auc": best.get("network_mean_roc_auc"),
        "baseline_f1": best.get("network_mean_baseline_f1"),
    }


def run_experiment_a(canonical):
    print("=" * 70)
    print("EXPERIMENT A: rain_occurrence persistence check")
    print("=" * 70)
    print()
    print("Persistence classifier: predict rain at T+h equals rain at T (column station_rain_event)")
    print("Compared against HGB best summary from Task 11 (network mean across LOSO folds)")
    print()

    rows = []
    for lead in LEADS:
        pers = experiment_a_per_lead(canonical, lead)
        hgb = load_rain_occurrence_hgb_summary(lead)
        if pers is None or hgb is None:
            continue
        rows.append({"lead": lead, "persistence": pers, "hgb": hgb})

    print(
        f"  {'lead':>4}  {'pos_rate':>9}  "
        f"{'pers_f1':>8}  {'pers_p':>7}  {'pers_r':>7}  "
        f"{'hgb_f1':>7}  {'hgb_p':>7}  {'hgb_r':>7}  "
        f"{'hgb_vs_pers_f1':>15}"
    )
    for r in rows:
        p = r["persistence"]
        h = r["hgb"]
        delta = h["f1_at_03"] - p["mean_persistence_f1"]
        print(
            f"  {r['lead']:>3}h  {p['mean_positive_rate']*100:>8.1f}%  "
            f"{p['mean_persistence_f1']:>8.3f}  {p['mean_persistence_precision']:>7.3f}  {p['mean_persistence_recall']:>7.3f}  "
            f"{h['f1_at_03']:>7.3f}  {h['precision_at_03']:>7.3f}  {h['recall_at_03']:>7.3f}  "
            f"{delta:>+14.3f}"
        )
    print()
    print("Reading the table:")
    print("  hgb_vs_pers_f1 > 0: HGB classifier beats persistence at this lead")
    print("  hgb_vs_pers_f1 < 0: persistence beats HGB; classifier is not adding value")
    print()
    return rows


def fit_hgb_rain_amount_with_features(framed, lead, holdout_station, feature_list_filter):
    target_col_log1p = f"rain_amount_log1p_lead_{lead}h"
    baseline_lead_col = f"{BASELINE_PRECIP_COLUMN}_lead_{lead}h"

    full_features = features_for("rain_amount", lead)
    filtered_features = [c for c in full_features if feature_list_filter(c)]
    present_features = [c for c in filtered_features if c in framed.columns]

    if len(present_features) == 0:
        raise RuntimeError("No features remain after filter")

    train_idx, val_idx = loso_split(framed, holdout_station)
    train_df = framed.iloc[train_idx].copy()
    val_df = framed.iloc[val_idx].copy()

    train_pos = train_df[np.expm1(train_df[target_col_log1p]) >= POSITIVE_RAIN_THRESHOLD_MM].copy()
    val_pos = val_df[np.expm1(val_df[target_col_log1p]) >= POSITIVE_RAIN_THRESHOLD_MM].copy()

    if len(train_pos) < 50 or len(val_pos) == 0:
        raise RuntimeError(f"Insufficient positive rows: train={len(train_pos)}, val={len(val_pos)}")

    stats = compute_imputation_stats(train_pos, present_features)
    train_pos = apply_imputation(train_pos, stats)
    val_pos = apply_imputation(val_pos, stats)

    X_train = train_pos[present_features].to_numpy(dtype=float)
    X_val = val_pos[present_features].to_numpy(dtype=float)
    y_train_log1p = train_pos[target_col_log1p].to_numpy(dtype=float)
    y_val_mm = np.expm1(val_pos[target_col_log1p].to_numpy(dtype=float))
    base_val_mm = val_pos[baseline_lead_col].to_numpy(dtype=float)
    persistence_val_mm = val_pos["rain_total_mm"].to_numpy(dtype=float) if "rain_total_mm" in val_pos.columns else np.full_like(y_val_mm, np.nan)

    keep_train = ~np.any(np.isnan(X_train), axis=1) & ~np.isnan(y_train_log1p)
    keep_val = (
        ~np.any(np.isnan(X_val), axis=1)
        & ~np.isnan(y_val_mm)
        & ~np.isnan(base_val_mm)
        & ~np.isnan(persistence_val_mm)
    )
    X_train, y_train_log1p = X_train[keep_train], y_train_log1p[keep_train]
    X_val = X_val[keep_val]
    y_val_mm = y_val_mm[keep_val]
    base_val_mm = base_val_mm[keep_val]
    persistence_val_mm = persistence_val_mm[keep_val]

    if len(X_train) == 0 or len(X_val) == 0:
        raise RuntimeError("Empty after NaN filter")

    model = HistGradientBoostingRegressor(
        max_depth=10, learning_rate=0.1, max_iter=500,
        early_stopping=True, validation_fraction=0.15,
        n_iter_no_change=25, random_state=42,
    )
    model.fit(X_train, y_train_log1p)
    pred_log1p = model.predict(X_val)
    pred_mm = np.clip(np.expm1(pred_log1p), 0.0, None)

    return {
        "n_train": int(len(X_train)),
        "n_val_positive": int(len(X_val)),
        "n_features_used": int(len(present_features)),
        "baseline_mae_mm": float(np.mean(np.abs(y_val_mm - base_val_mm))),
        "persistence_mae_mm": float(np.mean(np.abs(y_val_mm - persistence_val_mm))),
        "hgb_mae_mm": float(np.mean(np.abs(y_val_mm - pred_mm))),
        "actual_mean_mm": float(np.mean(y_val_mm)),
    }


def run_experiment_b(canonical):
    print("=" * 70)
    print("EXPERIMENT B: rain_amount HGB WITHOUT persistence features")
    print("=" * 70)
    print()
    print("Features removed:")
    for p in PERSISTENCE_FEATURE_PATTERNS:
        print(f"  - any feature matching '{p}*'")
    print()
    print("HGB trained only on validity-time forecast features + non-rain issue-time features")
    print()

    filter_fn = lambda c: not is_persistence_feature(c)

    print(f"  {'lead':>4}  {'n_feat':>6}  {'avg_n_val':>10}  "
          f"{'persist':>8}  {'om_base':>7}  {'hgb_nopers':>10}  "
          f"{'hgb_vs_om':>10}  {'hgb_vs_pers':>11}  {'time':>6}")

    all_results = []
    for lead in LEADS:
        t0 = time.time()
        framed = prepare_for_target(canonical, "rain_amount", lead)
        stations = sorted(framed["station_id"].unique())
        stations = [s for s in stations if s not in OVERRIDE_STATIONS]

        per_station = []
        n_features_used = None
        for sid in stations:
            try:
                result = fit_hgb_rain_amount_with_features(framed, lead, sid, filter_fn)
                per_station.append(result)
                n_features_used = result["n_features_used"]
            except Exception:
                continue
        elapsed = time.time() - t0
        if not per_station:
            print(f"  {lead:>3}h  no valid folds")
            continue

        n = len(per_station)
        avg_n_val = sum(r["n_val_positive"] for r in per_station) / n
        net_baseline_mae = sum(r["baseline_mae_mm"] for r in per_station) / n
        net_persistence_mae = sum(r["persistence_mae_mm"] for r in per_station) / n
        net_hgb_mae = sum(r["hgb_mae_mm"] for r in per_station) / n

        hgb_vs_om = 100.0 * (net_baseline_mae - net_hgb_mae) / net_baseline_mae if net_baseline_mae > 0 else 0.0
        hgb_vs_pers = 100.0 * (net_persistence_mae - net_hgb_mae) / net_persistence_mae if net_persistence_mae > 0 else 0.0

        print(
            f"  {lead:>3}h  {n_features_used:>6}  {avg_n_val:>10.0f}  "
            f"{net_persistence_mae:>8.3f}  {net_baseline_mae:>7.2f}  {net_hgb_mae:>10.3f}  "
            f"{hgb_vs_om:>+9.2f}%  {hgb_vs_pers:>+10.2f}%  {elapsed:>5.1f}s"
        )
        all_results.append({
            "lead": lead,
            "n_features": n_features_used,
            "n_stations": n,
            "avg_n_val_positive": avg_n_val,
            "persistence_mae_mm": net_persistence_mae,
            "baseline_mae_mm": net_baseline_mae,
            "hgb_no_persistence_mae_mm": net_hgb_mae,
            "hgb_vs_baseline_pct": hgb_vs_om,
            "hgb_vs_persistence_pct": hgb_vs_pers,
        })

    print()
    print("Reading the table:")
    print("  hgb_vs_om > 0:    HGB without persistence features beats Open-Meteo baseline")
    print("  hgb_vs_pers > 0:  HGB without persistence features beats naive persistence")
    print("  If hgb_vs_pers > 0 at any lead: validity-time features have real signal we can exploit")
    print()
    return all_results


def main():
    paths = get_paths()
    print("Loading canonical")
    canonical = load_canonical()
    print(f"  Loaded: {len(canonical):,} rows, {canonical['station_id'].nunique()} stations")
    print()

    exp_a = run_experiment_a(canonical)
    exp_b = run_experiment_b(canonical)

    output_dir = paths.reports.diagnostics_dir / "rain_experiments_ab"
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / "experiment_a_rain_occurrence_persistence.json").open("w") as f:
        json.dump(exp_a, f, indent=2)
    with (output_dir / "experiment_b_rain_amount_no_persistence.json").open("w") as f:
        json.dump(exp_b, f, indent=2)
    print(f"Saved: {output_dir}")


if __name__ == "__main__":
    main()
