from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

from postprocessing.training.data_loader import load_canonical, within_station_hourly_split
from postprocessing.training.feature_selection import features_for
from postprocessing.training.imputation import apply_imputation, compute_imputation_stats
from postprocessing.training.preparation import prepare_for_target
from postprocessing.utils.paths import get_paths


LEADS = (1, 6, 24, 48, 72)


WINNING_CONFIG = {
    "temperature":       {"model": "HGB",   "hp_str": "max_depth=10, lr=0.1"},
    "relative_humidity": {"model": "HGB",   "hp_str": "max_depth=10, lr=0.1"},
    "dew_point":         {"model": "HGB",   "hp_str": "max_depth=10, lr=0.1"},
    "wind_speed":        {"model": "Ridge", "hp_str": "alpha=10.0"},
    "wind_gust":         {"model": "Ridge", "hp_str": "alpha=0.1"},
    "pressure":          {"model": "Ridge", "hp_str": "alpha=0.1"},
    "uv":                {"model": "HGB",   "hp_str": "max_depth=10, lr=0.1"},
    "wind_direction":    {"model": "Ridge", "hp_str": "alpha=10.0"},
}


def _angle_from_sin_cos(sin_vals, cos_vals):
    return np.mod(np.degrees(np.arctan2(sin_vals, cos_vals)), 360.0)


def _circular_mae(predicted_deg, actual_deg):
    d = np.mod(predicted_deg - actual_deg + 180.0, 360.0) - 180.0
    return float(np.mean(np.abs(d)))


def _safe(d, *keys):
    cur = d
    for k in keys:
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(k)
        else:
            return None
    return cur


def load_loso_for_target(target):
    paths = get_paths()
    model_class = WINNING_CONFIG[target]["model"]

    if target == "wind_direction":
        base = paths.reports.diagnostics_dir / "ridge_loso_sweep_v2"
    elif model_class == "Ridge":
        base = paths.reports.diagnostics_dir / "ridge_loso_sweep"
    else:
        base = paths.reports.diagnostics_dir / "hgb_loso_sweep"

    out = {}
    for lead in LEADS:
        path = base / f"{target}_lead{lead}.json"
        if not path.exists():
            continue
        with path.open() as f:
            data = json.load(f)
        best = _safe(data, "summary", "best_summary") or {}
        if target == "uv":
            out[lead] = {
                "absolute_mae": best.get("network_mean_absolute_mae"),
                "target_mean": best.get("network_mean_target_mean"),
                "target_std": best.get("network_mean_target_std"),
                "n_stations": best.get("n_stations"),
            }
        elif target == "wind_direction":
            out[lead] = {
                "baseline_mae": best.get("network_mean_baseline_mae_deg"),
                "corrected_mae": best.get("network_mean_corrected_mae_deg"),
                "mae_reduction_pct": best.get("network_mean_mae_reduction_pct"),
                "n_failures": best.get("n_failure_stations"),
                "n_stations": best.get("n_stations"),
            }
        else:
            out[lead] = {
                "baseline_mae": best.get("network_mean_baseline_mae"),
                "corrected_mae": best.get("network_mean_corrected_mae"),
                "mae_reduction_pct": best.get("network_mean_mae_reduction_pct"),
                "baseline_bias": best.get("network_mean_baseline_bias"),
                "corrected_bias": best.get("network_mean_corrected_bias"),
                "bias_correction_pct": best.get("network_bias_correction_pct"),
                "n_failures": best.get("n_failure_stations"),
                "n_stations": best.get("n_stations"),
            }
    return out


def load_corrected_wind_direction_within_station():
    canonical = load_canonical()
    out = {}
    for lead in LEADS:
        framed = prepare_for_target(canonical, "wind_direction", lead)
        sin_col = f"winddir_residual_sin_lead_{lead}h"
        cos_col = f"winddir_residual_cos_lead_{lead}h"
        base_sin_col = f"base_wind_direction_sin_lead_{lead}h"
        base_cos_col = f"base_wind_direction_cos_lead_{lead}h"
        present_features = [c for c in features_for("wind_direction", lead) if c in framed.columns]

        train_idx, val_idx = within_station_hourly_split(
            framed,
            fraction=0.10,
            block_hours=48,
            seed=42,
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
            & ~np.isnan(y_train_sin)
            & ~np.isnan(y_train_cos)
        )
        keep_val = (
            ~np.any(np.isnan(X_val), axis=1)
            & ~np.isnan(y_val_sin)
            & ~np.isnan(y_val_cos)
            & ~np.isnan(base_sin)
            & ~np.isnan(base_cos)
        )

        X_train = X_train[keep_train]
        y_train = np.column_stack([y_train_sin[keep_train], y_train_cos[keep_train]])
        X_val = X_val[keep_val]
        y_val_sin = y_val_sin[keep_val]
        y_val_cos = y_val_cos[keep_val]
        base_sin = base_sin[keep_val]
        base_cos = base_cos[keep_val]

        scaler = StandardScaler()
        X_train_s = scaler.fit_transform(X_train)
        X_val_s = scaler.transform(X_val)
        model = Ridge(alpha=10.0)
        model.fit(X_train_s, y_train)
        pred = model.predict(X_val_s)

        base_angle = _angle_from_sin_cos(base_sin, base_cos)
        actual_residual_angle = np.degrees(np.arctan2(y_val_sin, y_val_cos))
        actual_angle = np.mod(base_angle + actual_residual_angle, 360.0)
        pred_residual_angle = np.degrees(np.arctan2(pred[:, 0], pred[:, 1]))
        corrected_angle = np.mod(base_angle + pred_residual_angle, 360.0)

        baseline_mae = _circular_mae(base_angle, actual_angle)
        corrected_mae = _circular_mae(corrected_angle, actual_angle)
        out[lead] = {
            "baseline_mae": baseline_mae,
            "corrected_mae": corrected_mae,
            "mae_reduction_pct": 100.0 * (baseline_mae - corrected_mae) / baseline_mae if baseline_mae > 0 else 0.0,
            "n_eval": int(len(X_val)),
        }
    return out


def load_within_station_for_target(target):
    if target == "wind_direction":
        return load_corrected_wind_direction_within_station()

    paths = get_paths()
    path = paths.reports.diagnostics_dir / "within_station_validation" / f"{target}_within_station.json"
    if not path.exists():
        return {}
    with path.open() as f:
        data = json.load(f)
    out = {}
    for lead_str, r in data.get("results_by_lead", {}).items():
        try:
            lead = int(lead_str)
        except ValueError:
            continue
        if "error" in r:
            continue
        if target == "uv":
            out[lead] = {
                "absolute_mae": r.get("corrected_mae"),
                "n_eval": r.get("n_val"),
            }
        elif target == "wind_direction":
            out[lead] = {
                "baseline_mae": r.get("baseline_mae_deg"),
                "corrected_mae": r.get("corrected_mae_deg"),
                "mae_reduction_pct": r.get("mae_reduction_pct"),
                "n_eval": r.get("n_val"),
            }
        else:
            out[lead] = {
                "baseline_mae": r.get("baseline_mae"),
                "corrected_mae": r.get("corrected_mae"),
                "mae_reduction_pct": r.get("mae_reduction_pct"),
                "baseline_bias": r.get("baseline_bias"),
                "corrected_bias": r.get("corrected_bias"),
                "n_eval": r.get("n_val"),
            }
    return out


def load_april_for_target(target):
    paths = get_paths()

    if target == "wind_direction":
        path = paths.reports.diagnostics_dir / "april_holdout" / "april_scores_v3_winddir_fix.json"
        if not path.exists():
            return {}
        with path.open() as f:
            data = json.load(f)
        out = {}
        for lead_str, r in data.items():
            try:
                lead = int(lead_str)
            except ValueError:
                continue
            if "error" in r:
                continue
            out[lead] = {
                "baseline_mae": r.get("baseline_mae_deg"),
                "corrected_mae": r.get("corrected_mae_deg"),
                "mae_reduction_pct": r.get("mae_reduction_pct"),
                "n_eval": r.get("n_eval"),
            }
        return out

    path = paths.reports.diagnostics_dir / "april_holdout" / "april_scores_v3.json"
    if not path.exists():
        return {}
    with path.open() as f:
        data = json.load(f)
    target_data = data.get(target, {})
    out = {}
    for lead_str, r in target_data.items():
        try:
            lead = int(lead_str)
        except ValueError:
            continue
        if "error" in r:
            continue
        if target == "uv":
            out[lead] = {
                "absolute_mae": r.get("absolute_mae"),
                "baseline_mae_for_reference": r.get("baseline_mae_for_reference"),
                "n_eval": r.get("n_eval"),
            }
        else:
            out[lead] = {
                "baseline_mae": r.get("baseline_mae"),
                "corrected_mae": r.get("corrected_mae"),
                "mae_reduction_pct": r.get("mae_reduction_pct"),
                "baseline_bias": r.get("baseline_bias"),
                "corrected_bias": r.get("corrected_bias"),
                "n_eval": r.get("n_eval"),
            }
    return out


def load_uv_normalized():
    paths = get_paths()
    path = paths.reports.diagnostics_dir / "uv_normalized_summary.json"
    if not path.exists():
        return None
    with path.open() as f:
        return json.load(f)


def build_master_csv():
    rows = []
    for target in WINNING_CONFIG:
        loso = load_loso_for_target(target)
        ws = load_within_station_for_target(target)
        apr = load_april_for_target(target)
        model_class = WINNING_CONFIG[target]["model"]
        hp_str = WINNING_CONFIG[target]["hp_str"]

        for mode_label, src in [("LOSO", loso), ("within_station", ws), ("april", apr)]:
            for lead in LEADS:
                entry = src.get(lead)
                if entry is None:
                    continue
                row = {
                    "target": target,
                    "model_class": model_class,
                    "hp": hp_str,
                    "validation_mode": mode_label,
                    "lead_hours": lead,
                    "shipped_in_v1": True,
                }
                for k in ("baseline_mae", "corrected_mae", "mae_reduction_pct",
                          "baseline_bias", "corrected_bias", "bias_correction_pct",
                          "absolute_mae", "baseline_mae_for_reference",
                          "target_mean", "target_std",
                          "n_eval", "n_stations", "n_failures"):
                    if k in entry:
                        row[k] = entry[k]
                rows.append(row)

    for target in ("rain_occurrence", "rain_amount"):
        rows.append({
            "target": target,
            "model_class": "investigated_not_shipped",
            "hp": "",
            "validation_mode": "see_rain_appendix",
            "lead_hours": None,
            "shipped_in_v1": False,
            "note": "see rain_appendix.md - persistence-dominated, excluded from v1",
        })

    df = pd.DataFrame(rows)
    return df


def build_summary_csv(master_df):
    rows = []
    for target in WINNING_CONFIG:
        model_class = WINNING_CONFIG[target]["model"]
        row = {"target": target, "model_class": model_class}
        for mode in ("LOSO", "within_station", "april"):
            for lead in LEADS:
                cell = master_df[
                    (master_df["target"] == target)
                    & (master_df["validation_mode"] == mode)
                    & (master_df["lead_hours"] == lead)
                ]
                if cell.empty:
                    val = None
                else:
                    if target == "uv":
                        val = cell["absolute_mae"].iloc[0] if "absolute_mae" in cell.columns else None
                    else:
                        val = cell["mae_reduction_pct"].iloc[0] if "mae_reduction_pct" in cell.columns else None
                row[f"{mode}_L{lead}"] = val
        if target == "uv":
            row["headline_metric"] = "absolute MAE (UV index points)"
        elif target == "wind_direction":
            row["headline_metric"] = "circular MAE reduction %"
        else:
            row["headline_metric"] = "MAE reduction %"
        rows.append(row)

    return pd.DataFrame(rows)


def main():
    output_dir = Path("reports/block2_final")
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Aggregating master metrics...")
    master = build_master_csv()
    master_path = output_dir / "master_metrics.csv"
    master.to_csv(master_path, index=False)
    print(f"  master: {master.shape}  -> {master_path}")

    print("Building summary table...")
    summary = build_summary_csv(master)
    summary_path = output_dir / "summary_table.csv"
    summary.to_csv(summary_path, index=False)
    print(f"  summary: {summary.shape}  -> {summary_path}")

    uv_norm = load_uv_normalized()
    if uv_norm is not None:
        uv_path = output_dir / "uv_normalized.json"
        with uv_path.open("w") as f:
            json.dump(uv_norm, f, indent=2)
        print(f"  uv normalized: {uv_path}")

    print()
    print("=== Master CSV preview ===")
    print(f"  rows: {len(master)}  cols: {len(master.columns)}")
    print(f"  columns: {sorted(master.columns.tolist())}")
    print()
    print("=== Summary table preview ===")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
