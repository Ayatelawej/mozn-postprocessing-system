from __future__ import annotations

import json
import time

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

from postprocessing.training.data_loader import (
    load_canonical,
    loso_split,
    target_columns_for,
)
from postprocessing.training.feature_selection import features_for
from postprocessing.training.hgb_runner import HGBConfig
from postprocessing.training.imputation import apply_imputation, compute_imputation_stats
from postprocessing.training.preparation import prepare_for_target
from postprocessing.training.ridge_runner import baseline_column_for
from postprocessing.utils.paths import get_paths


TARGET: str = "uv"
LEADS: tuple[int, ...] = (1, 6, 24, 48, 72)
ALPHAS: tuple[float, ...] = (0.1, 1.0, 10.0)
HGB_CONFIGS: tuple[HGBConfig, ...] = (
    HGBConfig(max_depth=3, learning_rate=0.1),
    HGBConfig(max_depth=6, learning_rate=0.1),
    HGBConfig(max_depth=10, learning_rate=0.1),
)


def ridge_sweep(canonical, lead, stations):
    framed = prepare_for_target(canonical, TARGET, lead)
    target_col = f"{target_columns_for(TARGET)[0]}_lead_{lead}h"
    feature_list = features_for(TARGET, lead)
    present_features = [c for c in feature_list if c in framed.columns]

    results = []
    for station in stations:
        for alpha in ALPHAS:
            try:
                train_idx, val_idx = loso_split(framed, station)
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

                if len(X_train) == 0 or len(X_val) == 0:
                    raise RuntimeError("empty after NaN filter")

                scaler = StandardScaler()
                X_train_scaled = scaler.fit_transform(X_train)
                X_val_scaled = scaler.transform(X_val)
                model = Ridge(alpha=alpha)
                model.fit(X_train_scaled, y_train)
                y_pred = model.predict(X_val_scaled)

                absolute_mae = float(np.mean(np.abs(y_val - y_pred)))
                absolute_rmse = float(np.sqrt(np.mean((y_val - y_pred) ** 2)))
                absolute_bias = float(np.mean(y_val - y_pred))
                target_mean = float(np.mean(y_val))
                target_std = float(np.std(y_val))

                results.append({
                    "target": TARGET, "lead": lead, "holdout_station": station,
                    "alpha": alpha, "n_train": int(len(X_train)), "n_val": int(len(X_val)),
                    "absolute_mae": absolute_mae,
                    "absolute_rmse": absolute_rmse,
                    "absolute_bias": absolute_bias,
                    "target_mean": target_mean,
                    "target_std": target_std,
                })
            except Exception as exc:
                results.append({
                    "target": TARGET, "lead": lead,
                    "holdout_station": station, "alpha": alpha,
                    "error": str(exc),
                })
    return results, len(framed)


def hgb_sweep(canonical, lead, stations):
    framed = prepare_for_target(canonical, TARGET, lead)
    target_col = f"{target_columns_for(TARGET)[0]}_lead_{lead}h"
    feature_list = features_for(TARGET, lead)
    present_features = [c for c in feature_list if c in framed.columns]

    results = []
    for station in stations:
        for cfg in HGB_CONFIGS:
            try:
                train_idx, val_idx = loso_split(framed, station)
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

                if len(X_train) == 0 or len(X_val) == 0:
                    raise RuntimeError("empty after NaN filter")

                model = HistGradientBoostingRegressor(
                    max_depth=cfg.max_depth, learning_rate=cfg.learning_rate,
                    max_iter=cfg.max_iter, early_stopping=cfg.early_stopping,
                    validation_fraction=cfg.validation_fraction,
                    n_iter_no_change=cfg.n_iter_no_change,
                    random_state=cfg.random_state,
                )
                model.fit(X_train, y_train)
                y_pred = model.predict(X_val)
                n_iter_used = int(getattr(model, "n_iter_", cfg.max_iter))

                absolute_mae = float(np.mean(np.abs(y_val - y_pred)))
                absolute_rmse = float(np.sqrt(np.mean((y_val - y_pred) ** 2)))
                absolute_bias = float(np.mean(y_val - y_pred))
                target_mean = float(np.mean(y_val))
                target_std = float(np.std(y_val))

                results.append({
                    "target": TARGET, "lead": lead, "holdout_station": station,
                    "config_label": cfg.label(), "config": cfg.as_dict(),
                    "n_train": int(len(X_train)), "n_val": int(len(X_val)),
                    "n_iter_used": n_iter_used,
                    "absolute_mae": absolute_mae,
                    "absolute_rmse": absolute_rmse,
                    "absolute_bias": absolute_bias,
                    "target_mean": target_mean,
                    "target_std": target_std,
                })
            except Exception as exc:
                results.append({
                    "target": TARGET, "lead": lead,
                    "holdout_station": station,
                    "config_label": cfg.label(), "config": cfg.as_dict(),
                    "error": str(exc),
                })
    return results, len(framed)


def summarize_by_hp(results, hp_field, hp_values):
    valid = [r for r in results if "error" not in r]
    if not valid:
        return {"error": "all_folds_failed"}
    by_hp = {}
    for v in hp_values:
        key = v.label() if hasattr(v, "label") else v
        rows = [r for r in valid if r[hp_field] == key]
        if not rows:
            continue
        n = len(rows)
        mean_abs_mae = sum(r["absolute_mae"] for r in rows) / n
        mean_abs_rmse = sum(r["absolute_rmse"] for r in rows) / n
        mean_abs_bias = sum(r["absolute_bias"] for r in rows) / n
        mean_target_mean = sum(r["target_mean"] for r in rows) / n
        mean_target_std = sum(r["target_std"] for r in rows) / n

        sorted_rows = sorted(rows, key=lambda r: r["absolute_mae"])
        best = sorted_rows[0]
        worst = sorted_rows[-1]
        median = sorted_rows[len(sorted_rows) // 2]

        def station_summary(r):
            return {
                "station_id": r["holdout_station"],
                "absolute_mae": r["absolute_mae"],
                "absolute_rmse": r["absolute_rmse"],
                "absolute_bias": r["absolute_bias"],
            }

        by_hp[key] = {
            "n_stations": n,
            "network_mean_absolute_mae": mean_abs_mae,
            "network_mean_absolute_rmse": mean_abs_rmse,
            "network_mean_absolute_bias": mean_abs_bias,
            "network_mean_target_mean": mean_target_mean,
            "network_mean_target_std": mean_target_std,
            "best_station": station_summary(best),
            "worst_station": station_summary(worst),
            "median_station": station_summary(median),
        }
    if not by_hp:
        return {"error": "no_valid_hp"}
    best_key = min(by_hp, key=lambda k: by_hp[k]["network_mean_absolute_mae"])
    out = {"by_hp": by_hp, "best_summary": by_hp[best_key]}
    if hp_field == "alpha":
        out["best_alpha"] = best_key
    else:
        out["best_config_label"] = best_key
    return out


def main():
    paths = get_paths()
    print("Loading canonical")
    df = load_canonical()
    stations = sorted(df["station_id"].unique())
    print(f"  Loaded: {len(df):,} rows, {len(stations)} stations")
    print()
    print("UV reporting uses absolute MAE against uv_index (units: UV-index points)")
    print("Open-Meteo does not serve UV in archive, so no baseline-relative metric")
    print()

    ridge_dir = paths.reports.diagnostics_dir / "ridge_loso_sweep"
    hgb_dir = paths.reports.diagnostics_dir / "hgb_loso_sweep"
    ridge_dir.mkdir(parents=True, exist_ok=True)
    hgb_dir.mkdir(parents=True, exist_ok=True)

    print("=== Ridge UV sweep ===")
    ridge_start = time.time()
    for lead in LEADS:
        t0 = time.time()
        results, framed_rows = ridge_sweep(df, lead, stations)
        elapsed = time.time() - t0
        summary = summarize_by_hp(results, "alpha", ALPHAS)
        if "error" not in summary:
            b = summary["best_summary"]
            print(
                f"  lead={lead:>2}h  framed={framed_rows:>6,}  "
                f"best_alpha={summary['best_alpha']:<5}  "
                f"abs_mae={b['network_mean_absolute_mae']:>6.3f}  "
                f"abs_rmse={b['network_mean_absolute_rmse']:>6.3f}  "
                f"abs_bias={b['network_mean_absolute_bias']:>+6.3f}  "
                f"uv_index_mean={b['network_mean_target_mean']:>5.2f}  "
                f"uv_index_std={b['network_mean_target_std']:>5.2f}  "
                f"time={elapsed:.1f}s"
            )
        else:
            print(f"  lead={lead}h  ERROR: {summary['error']}")
        path = ridge_dir / f"{TARGET}_lead{lead}.json"
        with path.open("w") as f:
            json.dump({
                "target": TARGET, "lead": lead, "alphas": list(ALPHAS),
                "stations": stations, "results": results, "summary": summary,
                "metric_type": "absolute_uv_index",
            }, f, indent=2)
    print(f"  Ridge total: {time.time() - ridge_start:.1f}s")
    print()

    print("=== HGB UV sweep ===")
    hgb_start = time.time()
    for lead in LEADS:
        t0 = time.time()
        results, framed_rows = hgb_sweep(df, lead, stations)
        elapsed = time.time() - t0
        summary = summarize_by_hp(results, "config_label", HGB_CONFIGS)
        if "error" not in summary:
            b = summary["best_summary"]
            print(
                f"  lead={lead:>2}h  framed={framed_rows:>6,}  "
                f"best={summary['best_config_label']:<10}  "
                f"abs_mae={b['network_mean_absolute_mae']:>6.3f}  "
                f"abs_rmse={b['network_mean_absolute_rmse']:>6.3f}  "
                f"abs_bias={b['network_mean_absolute_bias']:>+6.3f}  "
                f"time={elapsed:.1f}s"
            )
        else:
            print(f"  lead={lead}h  ERROR: {summary['error']}")
        path = hgb_dir / f"{TARGET}_lead{lead}.json"
        with path.open("w") as f:
            json.dump({
                "target": TARGET, "lead": lead,
                "configs": [c.as_dict() for c in HGB_CONFIGS],
                "stations": stations, "results": results, "summary": summary,
                "metric_type": "absolute_uv_index",
            }, f, indent=2)
    print(f"  HGB total: {time.time() - hgb_start:.1f}s")
    print()
    print(f"=== UV sweep complete ===")


if __name__ == "__main__":
    main()
