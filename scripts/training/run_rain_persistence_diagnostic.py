from __future__ import annotations

import json

import numpy as np
from sklearn.ensemble import HistGradientBoostingRegressor

from postprocessing.training.data_loader import load_canonical
from postprocessing.training.preparation import prepare_for_target
from postprocessing.training.rain_amount_runner import _prepare_xy_conditional
from postprocessing.utils.paths import get_paths


LEADS: tuple[int, ...] = (1, 6, 24, 48, 72)
POSITIVE_RAIN_THRESHOLD_MM: float = 0.1
OVERRIDE_STATIONS: set[str] = {"IMURQU7"}
FEATURE_IMPORTANCE_HOLDOUT: str = "IZAWIY5"
FEATURE_IMPORTANCE_LEAD: int = 1
N_PERMUTATION_REPEATS: int = 5
HGB_CONFIG: dict = {
    "max_depth": 10,
    "learning_rate": 0.1,
    "max_iter": 500,
    "early_stopping": True,
    "validation_fraction": 0.15,
    "n_iter_no_change": 25,
    "random_state": 42,
}


def compute_persistence_per_station(framed, lead, station):
    log1p_col = f"rain_amount_log1p_lead_{lead}h"
    if log1p_col not in framed.columns or "rain_total_mm" not in framed.columns:
        return None
    station_rows = framed[framed["station_id"] == station]
    if len(station_rows) == 0:
        return None
    actual_mm = np.expm1(station_rows[log1p_col].to_numpy(dtype=float))
    persistence_pred = station_rows["rain_total_mm"].to_numpy(dtype=float)
    keep = (
        ~np.isnan(actual_mm)
        & (actual_mm >= POSITIVE_RAIN_THRESHOLD_MM)
        & ~np.isnan(persistence_pred)
    )
    if keep.sum() == 0:
        return None
    actual_pos = actual_mm[keep]
    persistence_pos = persistence_pred[keep]
    mae = float(np.mean(np.abs(actual_pos - persistence_pos)))
    return {
        "n_positive": int(keep.sum()),
        "persistence_mae_mm": mae,
        "actual_mean_mm": float(np.mean(actual_pos)),
        "persistence_mean_mm": float(np.mean(persistence_pos)),
    }


def load_task12_metrics(lead):
    paths = get_paths()
    path = paths.reports.diagnostics_dir / "rain_amount_hgb_sweep" / f"rain_amount_lead{lead}.json"
    if not path.exists():
        return None
    with path.open() as f:
        data = json.load(f)
    best = data.get("summary", {}).get("best_summary", {})
    return {
        "baseline_mae_mm": best.get("network_mean_baseline_mae_mm"),
        "corrected_mae_mm": best.get("network_mean_corrected_mae_mm"),
    }


def compute_persistence_table(canonical):
    table = []
    for lead in LEADS:
        framed = prepare_for_target(canonical, "rain_amount", lead)
        stations = [s for s in framed["station_id"].unique() if s not in OVERRIDE_STATIONS]
        per_station = []
        for sid in stations:
            stats = compute_persistence_per_station(framed, lead, sid)
            if stats is not None:
                per_station.append(stats)
        if not per_station:
            continue
        n = len(per_station)
        net_persistence = sum(s["persistence_mae_mm"] for s in per_station) / n
        net_actual = sum(s["actual_mean_mm"] for s in per_station) / n
        task12 = load_task12_metrics(lead) or {}
        baseline = task12.get("baseline_mae_mm")
        hgb = task12.get("corrected_mae_mm")

        def reduction(a, b):
            return 100.0 * (a - b) / a if (a is not None and a > 0) else None

        table.append({
            "lead": lead,
            "n_stations": n,
            "actual_mean_mm": net_actual,
            "persistence_mae_mm": net_persistence,
            "baseline_mae_mm": baseline,
            "hgb_mae_mm": hgb,
            "persistence_vs_baseline_pct": reduction(baseline, net_persistence) if baseline else None,
            "hgb_vs_baseline_pct": reduction(baseline, hgb) if (baseline and hgb is not None) else None,
            "hgb_vs_persistence_pct": reduction(net_persistence, hgb) if (hgb is not None) else None,
        })
    return table


def manual_permutation_importance(model, X_val, y_val_mm, feature_names, n_repeats, seed=42):
    rng = np.random.default_rng(seed)
    base_pred_log1p = model.predict(X_val)
    base_pred_mm = np.clip(np.expm1(base_pred_log1p), 0.0, None)
    base_mae = float(np.mean(np.abs(y_val_mm - base_pred_mm)))
    importances = []
    for i, feat in enumerate(feature_names):
        deltas = []
        for _ in range(n_repeats):
            X_shuffled = X_val.copy()
            rng.shuffle(X_shuffled[:, i])
            pred_log1p = model.predict(X_shuffled)
            pred_mm = np.clip(np.expm1(pred_log1p), 0.0, None)
            shuffled_mae = float(np.mean(np.abs(y_val_mm - pred_mm)))
            deltas.append(shuffled_mae - base_mae)
        importances.append({
            "feature": feat,
            "mean_importance": float(np.mean(deltas)),
            "std_importance": float(np.std(deltas)),
        })
    importances.sort(key=lambda x: -x["mean_importance"])
    return base_mae, importances


def compute_feature_importance(canonical, lead, holdout_station, n_repeats):
    framed = prepare_for_target(canonical, "rain_amount", lead)
    prep = _prepare_xy_conditional(framed, lead, holdout_station)
    X_train, y_train_log1p, X_val, y_val_mm = prep[0], prep[1], prep[2], prep[3]
    present_features = prep[9]
    model = HistGradientBoostingRegressor(**HGB_CONFIG)
    model.fit(X_train, y_train_log1p)
    base_mae, importances = manual_permutation_importance(
        model, X_val, y_val_mm, present_features, n_repeats=n_repeats, seed=42,
    )
    return {
        "lead": lead,
        "holdout_station": holdout_station,
        "n_train": int(len(X_train)),
        "n_val_positive": int(len(X_val)),
        "base_mae_mm": base_mae,
        "n_features": len(present_features),
        "importances": importances,
    }


def main():
    paths = get_paths()
    print("Loading canonical")
    canonical = load_canonical()
    print(f"  Loaded: {len(canonical):,} rows, {canonical['station_id'].nunique()} stations")
    print()

    print("=== Rain persistence vs Open-Meteo baseline vs HGB ===")
    print("Persistence rule: predict rain at T+h equals rain at T (column rain_total_mm)")
    print(f"Restricted to rows where actual rain at T+h >= {POSITIVE_RAIN_THRESHOLD_MM} mm")
    print("HGB numbers loaded from existing Task 12 results")
    print()

    table = compute_persistence_table(canonical)

    print(
        f"  {'lead':>4}  {'n_st':>4}  {'actual':>7}  "
        f"{'persist':>8}  {'om_base':>7}  {'hgb':>7}  "
        f"{'pers_vs_om':>11}  {'hgb_vs_om':>11}  {'hgb_vs_pers':>12}"
    )

    def pct_or_dash(v):
        return f"{v:>+10.2f}%" if v is not None else f"{'---':>11}"

    for row in table:
        print(
            f"  {row['lead']:>3}h  {row['n_stations']:>4}  "
            f"{row['actual_mean_mm']:>7.3f}  "
            f"{row['persistence_mae_mm']:>8.3f}  "
            f"{row['baseline_mae_mm']:>7.2f}  "
            f"{row['hgb_mae_mm']:>7.3f}  "
            f"{pct_or_dash(row['persistence_vs_baseline_pct'])}  "
            f"{pct_or_dash(row['hgb_vs_baseline_pct'])}  "
            f"{pct_or_dash(row['hgb_vs_persistence_pct'])}"
        )
    print()
    print("Reading the table:")
    print("  pers_vs_om > 0%: persistence beats Open-Meteo (rain forecasting is hard)")
    print("  hgb_vs_pers > 0%: HGB adds skill on top of persistence (real post-processing)")
    print("  hgb_vs_pers <= 0%: HGB is just doing persistence; no real forecasting added")
    print()

    print(f"=== HGB feature importance (rain_amount, lead={FEATURE_IMPORTANCE_LEAD}, holdout={FEATURE_IMPORTANCE_HOLDOUT}) ===")
    print(f"Permutation importance ({N_PERMUTATION_REPEATS} repeats per feature)")
    print()

    fi = compute_feature_importance(
        canonical, FEATURE_IMPORTANCE_LEAD, FEATURE_IMPORTANCE_HOLDOUT, N_PERMUTATION_REPEATS,
    )
    print(f"Train rows: {fi['n_train']:,}, val positive rows: {fi['n_val_positive']:,}")
    print(f"Base MAE (mm) on val: {fi['base_mae_mm']:.3f}")
    print(f"Number of features: {fi['n_features']}")
    print()
    print(f"  {'rank':>4}  {'feature':<42}  {'importance_mm':>13}  {'std':>8}")
    for rank, item in enumerate(fi["importances"][:15], 1):
        print(f"  {rank:>4}  {item['feature']:<42}  {item['mean_importance']:>+13.4f}  {item['std_importance']:>8.4f}")
    print()
    print("Reading the importance:")
    print("  Importance = how much MAE worsens (in mm) when that feature is shuffled")
    print("  If station_rain_rolling_*, rain_total_mm_lag_*, RH lags dominate -> persistence-style")
    print("  If base_precipitation_mm_lead_1h, base cloud/pressure dominate -> real post-processing")
    print()

    output_dir = paths.reports.diagnostics_dir / "rain_persistence_diagnostic"
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / "persistence_comparison.json").open("w") as f:
        json.dump({"persistence_table": table}, f, indent=2)
    with (output_dir / "feature_importance.json").open("w") as f:
        json.dump(fi, f, indent=2)
    print(f"Saved: {output_dir / 'persistence_comparison.json'}")
    print(f"Saved: {output_dir / 'feature_importance.json'}")


if __name__ == "__main__":
    main()
