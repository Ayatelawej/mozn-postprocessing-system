from __future__ import annotations

import json
import time

import numpy as np

from postprocessing.training.data_loader import load_canonical
from postprocessing.training.hgb_runner import HGBConfig, fit_hgb
from postprocessing.training.preparation import prepare_for_target
from postprocessing.utils.paths import get_paths


TARGETS: tuple[str, ...] = (
    "temperature", "relative_humidity", "dew_point",
    "wind_speed", "wind_gust", "pressure",
)
LEAD: int = 1

CONFIGS: tuple[HGBConfig, ...] = (
    HGBConfig(max_depth=3, learning_rate=0.05),
    HGBConfig(max_depth=3, learning_rate=0.1),
    HGBConfig(max_depth=6, learning_rate=0.05),
    HGBConfig(max_depth=6, learning_rate=0.1),
    HGBConfig(max_depth=10, learning_rate=0.05),
    HGBConfig(max_depth=10, learning_rate=0.1),
)


def sweep_target(canonical, target, lead, stations, configs):
    framed = prepare_for_target(canonical, target, lead)
    results = []
    for station in stations:
        for cfg in configs:
            try:
                result, _ = fit_hgb(framed, target, lead, station, config=cfg)
                results.append(result.as_dict())
            except Exception as exc:
                results.append({
                    "target": target,
                    "lead": lead,
                    "holdout_station": station,
                    "config_label": cfg.label(),
                    "config": cfg.as_dict(),
                    "error": str(exc),
                })
    return results, len(framed)


def summarize_target(results, configs):
    valid = [r for r in results if "error" not in r]
    if not valid:
        return {"error": "all_folds_failed"}

    by_config = {}
    for cfg in configs:
        rows = [r for r in valid if r["config_label"] == cfg.label()]
        if not rows:
            continue
        n = len(rows)
        mean_base_mae = sum(r["baseline_mae"] for r in rows) / n
        mean_corr_mae = sum(r["corrected_mae"] for r in rows) / n
        mean_mae_red = sum(r["mae_reduction_pct"] for r in rows) / n
        mean_base_bias = sum(r["baseline_bias"] for r in rows) / n
        mean_corr_bias = sum(r["corrected_bias"] for r in rows) / n

        if abs(mean_base_bias) >= 0.05 * mean_base_mae:
            network_bias_corr = 100.0 * (abs(mean_base_bias) - abs(mean_corr_bias)) / abs(mean_base_bias)
            bias_meaningful = True
        else:
            network_bias_corr = None
            bias_meaningful = False

        sorted_by_red = sorted(rows, key=lambda r: r["mae_reduction_pct"])
        worst = sorted_by_red[0]
        best = sorted_by_red[-1]
        median = sorted_by_red[len(sorted_by_red) // 2]

        def station_summary(r):
            return {
                "station_id": r["holdout_station"],
                "baseline_mae": r["baseline_mae"],
                "corrected_mae": r["corrected_mae"],
                "mae_reduction_pct": r["mae_reduction_pct"],
                "baseline_bias": r["baseline_bias"],
                "corrected_bias": r["corrected_bias"],
                "bias_correction_pct": r["bias_correction_pct"],
            }

        by_config[cfg.label()] = {
            "config": cfg.as_dict(),
            "n_stations": n,
            "n_failure_stations": sum(1 for r in rows if r["corrected_mae"] > r["baseline_mae"]),
            "network_mean_baseline_mae": mean_base_mae,
            "network_mean_corrected_mae": mean_corr_mae,
            "network_mean_mae_reduction_pct": mean_mae_red,
            "network_median_mae_reduction_pct": median["mae_reduction_pct"],
            "network_mean_baseline_bias": mean_base_bias,
            "network_mean_corrected_bias": mean_corr_bias,
            "network_bias_correction_pct": network_bias_corr,
            "bias_meaningful": bias_meaningful,
            "best_station": station_summary(best),
            "worst_station": station_summary(worst),
            "median_station": station_summary(median),
            "mean_n_iter_used": sum(r["n_iter_used"] for r in rows) / n,
        }

    if not by_config:
        return {"error": "no_valid_config"}

    best_label = max(by_config, key=lambda c: by_config[c]["network_mean_mae_reduction_pct"])
    return {
        "by_config": by_config,
        "best_config_label": best_label,
        "best_summary": by_config[best_label],
    }


def print_target_block(target, summary, framed_rows, elapsed):
    print(f"--- {target} ---")
    print(f"  Framed rows: {framed_rows:,}, sweep time: {elapsed:.1f}s")
    if "error" in summary:
        print(f"  ERROR: {summary['error']}")
        print()
        return
    b = summary["best_summary"]
    best = b["best_station"]
    worst = b["worst_station"]
    median = b["median_station"]
    print(f"  Best config: {summary['best_config_label']}  (mean_n_iter={b['mean_n_iter_used']:.0f})")
    print(f"  Network mean MAE reduction:    {b['network_mean_mae_reduction_pct']:+.2f}%")
    print(f"  Network median MAE reduction:  {b['network_median_mae_reduction_pct']:+.2f}%")
    print(f"  Best station:   {best['station_id']:>10}  mae_red={best['mae_reduction_pct']:+.2f}%  baseline_mae={best['baseline_mae']:.3f}  corrected_mae={best['corrected_mae']:.3f}")
    print(f"  Median station: {median['station_id']:>10}  mae_red={median['mae_reduction_pct']:+.2f}%")
    print(f"  Worst station:  {worst['station_id']:>10}  mae_red={worst['mae_reduction_pct']:+.2f}%  baseline_mae={worst['baseline_mae']:.3f}  corrected_mae={worst['corrected_mae']:.3f}")
    print(f"  Network baseline bias:  {b['network_mean_baseline_bias']:+.4f}")
    print(f"  Network corrected bias: {b['network_mean_corrected_bias']:+.4f}")
    if b["bias_meaningful"]:
        print(f"  Network bias correction: {b['network_bias_correction_pct']:+.2f}%")
    else:
        print(f"  Network bias correction: N/A (|baseline bias| < 5% of baseline MAE)")
    print(f"  Failure stations: {b['n_failure_stations']} / {b['n_stations']}")
    print()


def main() -> None:
    paths = get_paths()
    print("Loading canonical")
    df = load_canonical()
    stations = sorted(df["station_id"].unique())
    total_fits = len(TARGETS) * len(stations) * len(CONFIGS)
    print(f"  Loaded: {len(df):,} rows, {len(stations)} stations")
    print(f"  Targets: {len(TARGETS)}, configs per target: {len(CONFIGS)}, lead: {LEAD}")
    print(f"  Total fits to run: {total_fits}")
    for cfg in CONFIGS:
        print(f"    {cfg.label()}: {cfg.as_dict()}")
    print()

    all_results = {}
    all_summaries = {}
    output_dir = paths.reports.diagnostics_dir / "hgb_loso_sweep"
    output_dir.mkdir(parents=True, exist_ok=True)

    total_start = time.time()
    for target in TARGETS:
        t0 = time.time()
        results, framed_rows = sweep_target(df, target, LEAD, stations, CONFIGS)
        elapsed = time.time() - t0
        summary = summarize_target(results, CONFIGS)
        all_results[target] = results
        all_summaries[target] = summary
        print_target_block(target, summary, framed_rows, elapsed)

        path = output_dir / f"{target}_lead{LEAD}.json"
        with path.open("w") as f:
            json.dump({
                "target": target,
                "lead": LEAD,
                "configs": [c.as_dict() for c in CONFIGS],
                "stations": stations,
                "results": results,
                "summary": summary,
            }, f, indent=2)

    total_elapsed = time.time() - total_start
    print(f"=== All HGB sweeps complete in {total_elapsed:.1f}s ({total_elapsed/60:.1f} min) ===")
    print()

    print("=== Cross-target MAE reduction summary ===")
    print(
        f"  {'target':>20}  {'config':>10}  {'mean':>8}  {'median':>8}  "
        f"{'best_station':>22}  {'worst_station':>22}"
    )
    for target in TARGETS:
        s = all_summaries[target]
        if "error" in s:
            print(f"  {target:>20}  ERROR: {s['error']}")
            continue
        b = s["best_summary"]
        best_label = f"{b['best_station']['station_id']} ({b['best_station']['mae_reduction_pct']:+.1f}%)"
        worst_label = f"{b['worst_station']['station_id']} ({b['worst_station']['mae_reduction_pct']:+.1f}%)"
        print(
            f"  {target:>20}  {s['best_config_label']:>10}  "
            f"{b['network_mean_mae_reduction_pct']:>+7.2f}%  "
            f"{b['network_median_mae_reduction_pct']:>+7.2f}%  "
            f"{best_label:>22}  {worst_label:>22}"
        )
    print()

    print("=== Cross-target bias summary ===")
    print(
        f"  {'target':>20}  {'config':>10}  {'base_bias':>10}  {'corr_bias':>10}  "
        f"{'net_bias_corr':>14}  {'fail':>7}"
    )
    for target in TARGETS:
        s = all_summaries[target]
        if "error" in s:
            continue
        b = s["best_summary"]
        if b["bias_meaningful"]:
            bias_corr_str = f"{b['network_bias_correction_pct']:>+13.2f}%"
        else:
            bias_corr_str = f"{'N/A':>14}"
        fail_str = f"{b['n_failure_stations']}/{b['n_stations']}"
        print(
            f"  {target:>20}  {s['best_config_label']:>10}  "
            f"{b['network_mean_baseline_bias']:>+10.4f}  {b['network_mean_corrected_bias']:>+10.4f}  "
            f"{bias_corr_str}  {fail_str:>7}"
        )
    print()

    aggregate_path = output_dir / f"all_residual_targets_lead{LEAD}_summary.json"
    with aggregate_path.open("w") as f:
        json.dump({
            "lead": LEAD,
            "configs": [c.as_dict() for c in CONFIGS],
            "targets": list(TARGETS),
            "summaries": {t: all_summaries[t] for t in TARGETS},
        }, f, indent=2)
    print(f"Aggregate summary: {aggregate_path}")


if __name__ == "__main__":
    main()
