from __future__ import annotations

import json
import time

import numpy as np

from postprocessing.training.data_loader import load_canonical
from postprocessing.training.hgb_runner import HGBConfig
from postprocessing.training.preparation import prepare_for_target
from postprocessing.training.wind_direction_runner import (
    fit_hgb_wind_direction,
    fit_ridge_wind_direction,
)
from postprocessing.utils.paths import get_paths


TARGET: str = "wind_direction"
LEADS: tuple[int, ...] = (1, 6, 24, 48, 72)
ALPHAS: tuple[float, ...] = (0.1, 1.0, 10.0)
HGB_CONFIGS: tuple[HGBConfig, ...] = (
    HGBConfig(max_depth=3, learning_rate=0.1),
    HGBConfig(max_depth=6, learning_rate=0.1),
    HGBConfig(max_depth=10, learning_rate=0.1),
)


def ridge_sweep(canonical, lead, stations):
    framed = prepare_for_target(canonical, TARGET, lead)
    results = []
    for station in stations:
        for alpha in ALPHAS:
            try:
                result, _ = fit_ridge_wind_direction(framed, lead, station, alpha=alpha)
                results.append(result.as_dict())
            except Exception as exc:
                results.append({
                    "target": TARGET, "lead": lead,
                    "holdout_station": station, "hp_label": f"alpha{alpha}",
                    "error": str(exc),
                })
    return results, len(framed)


def hgb_sweep(canonical, lead, stations):
    framed = prepare_for_target(canonical, TARGET, lead)
    results = []
    for station in stations:
        for cfg in HGB_CONFIGS:
            try:
                result, _ = fit_hgb_wind_direction(framed, lead, station, config=cfg)
                results.append(result.as_dict())
            except Exception as exc:
                results.append({
                    "target": TARGET, "lead": lead,
                    "holdout_station": station, "hp_label": cfg.label(),
                    "error": str(exc),
                })
    return results, len(framed)


def summarize(results, hp_labels):
    valid = [r for r in results if "error" not in r]
    if not valid:
        return {"error": "all_folds_failed"}
    by_hp = {}
    for label in hp_labels:
        rows = [r for r in valid if r["hp_label"] == label]
        if not rows:
            continue
        n = len(rows)
        mean_base_mae = sum(r["baseline_circular_mae_deg"] for r in rows) / n
        mean_corr_mae = sum(r["corrected_circular_mae_deg"] for r in rows) / n
        mean_red = sum(r["mae_reduction_pct"] for r in rows) / n

        filt_rows = [r for r in rows if not (isinstance(r.get("baseline_circular_mae_filtered_deg"), float) and np.isnan(r["baseline_circular_mae_filtered_deg"]))]
        if filt_rows:
            mean_base_mae_filt = sum(r["baseline_circular_mae_filtered_deg"] for r in filt_rows) / len(filt_rows)
            mean_corr_mae_filt = sum(r["corrected_circular_mae_filtered_deg"] for r in filt_rows) / len(filt_rows)
            mean_red_filt = sum(r["mae_reduction_filtered_pct"] for r in filt_rows) / len(filt_rows)
        else:
            mean_base_mae_filt = float("nan")
            mean_corr_mae_filt = float("nan")
            mean_red_filt = float("nan")

        sorted_rows = sorted(rows, key=lambda r: -r["mae_reduction_pct"])
        best = sorted_rows[0]
        worst = sorted_rows[-1]
        median = sorted_rows[len(sorted_rows) // 2]

        def station_summary(r):
            return {
                "station_id": r["holdout_station"],
                "baseline_circular_mae_deg": r["baseline_circular_mae_deg"],
                "corrected_circular_mae_deg": r["corrected_circular_mae_deg"],
                "mae_reduction_pct": r["mae_reduction_pct"],
                "n_val": r["n_val"],
                "n_val_filtered": r["n_val_filtered"],
            }

        by_hp[label] = {
            "n_stations": n,
            "n_failure_stations": sum(1 for r in rows if r["corrected_circular_mae_deg"] > r["baseline_circular_mae_deg"]),
            "network_mean_baseline_mae_deg": mean_base_mae,
            "network_mean_corrected_mae_deg": mean_corr_mae,
            "network_mean_mae_reduction_pct": mean_red,
            "network_mean_baseline_mae_filtered_deg": mean_base_mae_filt,
            "network_mean_corrected_mae_filtered_deg": mean_corr_mae_filt,
            "network_mean_mae_reduction_filtered_pct": mean_red_filt,
            "best_station": station_summary(best),
            "worst_station": station_summary(worst),
            "median_station": station_summary(median),
        }
    if not by_hp:
        return {"error": "no_valid_hp"}
    best_label = max(by_hp, key=lambda k: by_hp[k]["network_mean_mae_reduction_pct"])
    return {"by_hp": by_hp, "best_hp_label": best_label, "best_summary": by_hp[best_label]}


def print_block(model_name, lead, summary, framed_rows, elapsed):
    if "error" in summary:
        print(f"  lead={lead}h  ERROR: {summary['error']}")
        return
    b = summary["best_summary"]
    print(
        f"  lead={lead:>2}h  framed={framed_rows:>6,}  "
        f"best={summary['best_hp_label']:<12}  "
        f"base_mae={b['network_mean_baseline_mae_deg']:>6.2f}deg  "
        f"corr_mae={b['network_mean_corrected_mae_deg']:>6.2f}deg  "
        f"red={b['network_mean_mae_reduction_pct']:>+6.2f}%  "
        f"filt(>3kmh) base={b['network_mean_baseline_mae_filtered_deg']:>5.1f}deg  "
        f"corr={b['network_mean_corrected_mae_filtered_deg']:>5.1f}deg  "
        f"red={b['network_mean_mae_reduction_filtered_pct']:>+6.2f}%  "
        f"fail={b['n_failure_stations']}/{b['n_stations']}  "
        f"time={elapsed:.1f}s"
    )


def main():
    paths = get_paths()
    print("Loading canonical")
    df = load_canonical()
    stations = sorted(df["station_id"].unique())
    print(f"  Loaded: {len(df):,} rows, {len(stations)} stations")
    print()
    print("Wind direction reports circular MAE in degrees")
    print("Filtered metric: only rows where station wind_speed_kmh >= 3.0")
    print()

    ridge_dir = paths.reports.diagnostics_dir / "ridge_loso_sweep"
    hgb_dir = paths.reports.diagnostics_dir / "hgb_loso_sweep"
    ridge_dir.mkdir(parents=True, exist_ok=True)
    hgb_dir.mkdir(parents=True, exist_ok=True)

    print("=== Ridge wind_direction sweep ===")
    ridge_start = time.time()
    for lead in LEADS:
        t0 = time.time()
        results, framed_rows = ridge_sweep(df, lead, stations)
        elapsed = time.time() - t0
        summary = summarize(results, [f"alpha{a}" for a in ALPHAS])
        print_block("Ridge", lead, summary, framed_rows, elapsed)
        path = ridge_dir / f"{TARGET}_lead{lead}.json"
        with path.open("w") as f:
            json.dump({
                "target": TARGET, "lead": lead, "alphas": list(ALPHAS),
                "stations": stations, "results": results, "summary": summary,
                "metric_type": "circular_mae_deg",
            }, f, indent=2)
    print(f"  Ridge total: {time.time() - ridge_start:.1f}s")
    print()

    print("=== HGB wind_direction sweep ===")
    hgb_start = time.time()
    for lead in LEADS:
        t0 = time.time()
        results, framed_rows = hgb_sweep(df, lead, stations)
        elapsed = time.time() - t0
        summary = summarize(results, [c.label() for c in HGB_CONFIGS])
        print_block("HGB", lead, summary, framed_rows, elapsed)
        path = hgb_dir / f"{TARGET}_lead{lead}.json"
        with path.open("w") as f:
            json.dump({
                "target": TARGET, "lead": lead,
                "configs": [c.as_dict() for c in HGB_CONFIGS],
                "stations": stations, "results": results, "summary": summary,
                "metric_type": "circular_mae_deg",
            }, f, indent=2)
    print(f"  HGB total: {time.time() - hgb_start:.1f}s")
    print()
    print("=== wind_direction sweep complete ===")


if __name__ == "__main__":
    main()
