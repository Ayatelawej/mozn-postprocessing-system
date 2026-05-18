from __future__ import annotations

import json
import time

from postprocessing.training.data_loader import load_canonical
from postprocessing.training.hgb_runner import HGBConfig, fit_hgb
from postprocessing.training.preparation import prepare_for_target
from postprocessing.utils.paths import get_paths


TARGETS: tuple[str, ...] = (
    "temperature", "relative_humidity", "dew_point",
    "wind_speed", "wind_gust", "pressure",
)
LEADS: tuple[int, ...] = (6, 24, 48, 72)
CONFIGS: tuple[HGBConfig, ...] = (
    HGBConfig(max_depth=3, learning_rate=0.1),
    HGBConfig(max_depth=6, learning_rate=0.1),
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


def main():
    paths = get_paths()
    print("Loading canonical")
    df = load_canonical()
    stations = sorted(df["station_id"].unique())
    print(f"  Loaded: {len(df):,} rows, {len(stations)} stations")
    print(f"  Targets: {len(TARGETS)}, leads: {LEADS}, configs: {len(CONFIGS)}")
    total = len(TARGETS) * len(LEADS) * len(stations) * len(CONFIGS)
    print(f"  Total fits: {total}")
    for cfg in CONFIGS:
        print(f"    {cfg.label()}: {cfg.as_dict()}")
    print()

    output_dir = paths.reports.diagnostics_dir / "hgb_loso_sweep"
    output_dir.mkdir(parents=True, exist_ok=True)

    total_start = time.time()
    for lead in LEADS:
        print(f"=== Lead = {lead}h ===")
        for target in TARGETS:
            t0 = time.time()
            results, framed_rows = sweep_target(df, target, lead, stations, CONFIGS)
            elapsed = time.time() - t0
            summary = summarize_target(results, CONFIGS)

            if "error" in summary:
                print(f"  {target:>20}  ERROR: {summary['error']}")
            else:
                b = summary["best_summary"]
                print(
                    f"  {target:>20}  framed={framed_rows:>6,}  "
                    f"best={summary['best_config_label']:<10}  "
                    f"mean_mae_red={b['network_mean_mae_reduction_pct']:>+7.2f}%  "
                    f"median={b['network_median_mae_reduction_pct']:>+7.2f}%  "
                    f"fail={b['n_failure_stations']}/{b['n_stations']}  "
                    f"time={elapsed:.1f}s"
                )

            path = output_dir / f"{target}_lead{lead}.json"
            with path.open("w") as f:
                json.dump({
                    "target": target,
                    "lead": lead,
                    "configs": [c.as_dict() for c in CONFIGS],
                    "stations": stations,
                    "results": results,
                    "summary": summary,
                }, f, indent=2)
        print()

    total_elapsed = time.time() - total_start
    print(f"=== HGB multilead sweep complete in {total_elapsed:.1f}s ({total_elapsed/60:.1f} min) ===")


if __name__ == "__main__":
    main()
