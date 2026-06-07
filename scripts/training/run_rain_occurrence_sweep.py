from __future__ import annotations

import json
import time

import numpy as np

from postprocessing.training.data_loader import load_canonical
from postprocessing.training.preparation import prepare_for_target
from postprocessing.training.rain_classifier_runner import (
    BASELINE_PRECIP_THRESHOLD_MM,
    HGBClassifierConfig,
    LogRegConfig,
    fit_hgb_classifier_rain,
    fit_logreg_rain,
)
from postprocessing.utils.paths import get_paths


TARGET: str = "rain_occurrence"
LEADS: tuple[int, ...] = (1, 6, 24, 48, 72)

LOGREG_CONFIGS: tuple[LogRegConfig, ...] = (
    LogRegConfig(C=0.1, class_weight="balanced"),
    LogRegConfig(C=1.0, class_weight="balanced"),
    LogRegConfig(C=10.0, class_weight="balanced"),
)

HGB_CONFIGS: tuple[HGBClassifierConfig, ...] = (
    HGBClassifierConfig(max_depth=3, learning_rate=0.1, class_weight="balanced"),
    HGBClassifierConfig(max_depth=6, learning_rate=0.1, class_weight="balanced"),
    HGBClassifierConfig(max_depth=10, learning_rate=0.1, class_weight="balanced"),
)


def logreg_sweep(canonical, lead, stations):
    framed = prepare_for_target(canonical, TARGET, lead)
    results = []
    for station in stations:
        for cfg in LOGREG_CONFIGS:
            try:
                result, _ = fit_logreg_rain(framed, lead, station, config=cfg)
                results.append(result.as_dict())
            except Exception as exc:
                results.append({
                    "target": TARGET, "lead": lead,
                    "holdout_station": station, "hp_label": cfg.label(),
                    "error": str(exc),
                })
    return results, len(framed)


def hgb_sweep(canonical, lead, stations):
    framed = prepare_for_target(canonical, TARGET, lead)
    results = []
    for station in stations:
        for cfg in HGB_CONFIGS:
            try:
                result, _ = fit_hgb_classifier_rain(framed, lead, station, config=cfg)
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
        return {"error": "all_folds_failed", "n_errors": len(results)}
    by_hp = {}
    for label in hp_labels:
        rows = [r for r in valid if r["hp_label"] == label]
        if not rows:
            continue
        n = len(rows)
        mean = lambda k: sum(r[k] for r in rows) / n

        avg_at_03 = {
            "precision": sum(r["metrics_at_threshold"]["0.3"]["precision"] for r in rows) / n,
            "recall": sum(r["metrics_at_threshold"]["0.3"]["recall"] for r in rows) / n,
            "f1": sum(r["metrics_at_threshold"]["0.3"]["f1"] for r in rows) / n,
        }

        sorted_rows = sorted(rows, key=lambda r: -r["f1_at_03"])
        best = sorted_rows[0]
        worst = sorted_rows[-1]
        median = sorted_rows[len(sorted_rows) // 2]

        def station_summary(r):
            return {
                "station_id": r["holdout_station"],
                "f1_at_03": r["f1_at_03"],
                "baseline_f1": r["baseline_f1"],
                "f1_improvement_at_03": r["f1_improvement_at_03"],
                "roc_auc": r["roc_auc"],
                "positive_rate_val": r["positive_rate_val"],
            }

        by_hp[label] = {
            "n_stations": n,
            "network_mean_baseline_f1": mean("baseline_f1"),
            "network_mean_baseline_precision": mean("baseline_precision"),
            "network_mean_baseline_recall": mean("baseline_recall"),
            "network_mean_roc_auc": mean("roc_auc"),
            "network_mean_average_precision": mean("average_precision"),
            "network_mean_brier_score": mean("brier_score"),
            "network_mean_f1_at_03": avg_at_03["f1"],
            "network_mean_precision_at_03": avg_at_03["precision"],
            "network_mean_recall_at_03": avg_at_03["recall"],
            "network_mean_f1_improvement_at_03": mean("f1_improvement_at_03"),
            "network_mean_positive_rate_val": mean("positive_rate_val"),
            "n_failure_stations": sum(1 for r in rows if r["f1_improvement_at_03"] <= 0),
            "best_station": station_summary(best),
            "worst_station": station_summary(worst),
            "median_station": station_summary(median),
        }
    if not by_hp:
        return {"error": "no_valid_hp"}
    best_label = max(by_hp, key=lambda k: by_hp[k]["network_mean_f1_at_03"])
    return {"by_hp": by_hp, "best_hp_label": best_label, "best_summary": by_hp[best_label]}


def print_block(model_name, lead, summary, framed_rows, elapsed):
    if "error" in summary:
        print(f"  lead={lead}h  ERROR: {summary['error']}")
        return
    b = summary["best_summary"]
    print(
        f"  lead={lead:>2}h  framed={framed_rows:>6,}  "
        f"best={summary['best_hp_label']:<18}  "
        f"pos_rate={b['network_mean_positive_rate_val']*100:>4.1f}%  "
        f"roc_auc={b['network_mean_roc_auc']:>5.3f}  "
        f"avg_prec={b['network_mean_average_precision']:>5.3f}  "
        f"base_f1={b['network_mean_baseline_f1']:>5.3f}  "
        f"model_f1@0.3={b['network_mean_f1_at_03']:>5.3f}  "
        f"(p={b['network_mean_precision_at_03']:>5.3f}, r={b['network_mean_recall_at_03']:>5.3f})  "
        f"f1_gain={b['network_mean_f1_improvement_at_03']:>+5.3f}  "
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
    print("rain_occurrence as binary classification, probability output")
    print(f"Baseline = base_precipitation_mm > {BASELINE_PRECIP_THRESHOLD_MM} mm (thresholded Open-Meteo)")
    print("Winner selected by network-mean F1 at threshold 0.3")
    print()
    print(f"Baseline threshold confirmed: {BASELINE_PRECIP_THRESHOLD_MM} mm")
    print()

    logreg_dir = paths.reports.diagnostics_dir / "rain_logreg_sweep"
    hgb_dir = paths.reports.diagnostics_dir / "rain_hgb_classifier_sweep"
    logreg_dir.mkdir(parents=True, exist_ok=True)
    hgb_dir.mkdir(parents=True, exist_ok=True)

    print("=== LogisticRegression rain_occurrence sweep ===")
    logreg_start = time.time()
    for lead in LEADS:
        t0 = time.time()
        results, framed_rows = logreg_sweep(df, lead, stations)
        elapsed = time.time() - t0
        summary = summarize(results, [c.label() for c in LOGREG_CONFIGS])
        print_block("LogReg", lead, summary, framed_rows, elapsed)
        path = logreg_dir / f"{TARGET}_lead{lead}.json"
        with path.open("w") as f:
            json.dump({
                "target": TARGET, "lead": lead,
                "configs": [c.as_dict() for c in LOGREG_CONFIGS],
                "stations": stations, "results": results, "summary": summary,
                "metric_type": "classification_f1",
            }, f, indent=2)
    print(f"  LogReg total: {time.time() - logreg_start:.1f}s")
    print()

    print("=== HGB Classifier rain_occurrence sweep ===")
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
                "metric_type": "classification_f1",
            }, f, indent=2)
    print(f"  HGB total: {time.time() - hgb_start:.1f}s")
    print()
    print("=== rain_occurrence sweep complete ===")


if __name__ == "__main__":
    main()
