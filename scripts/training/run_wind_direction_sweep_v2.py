from __future__ import annotations

import json
import time

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
    return results


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
    return results


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
        mean_base = sum(r["baseline_circular_mae_deg"] for r in rows) / n
        mean_corr = sum(r["corrected_circular_mae_deg"] for r in rows) / n
        mean_red = sum(r["mae_reduction_pct"] for r in rows) / n
        by_hp[label] = {
            "n_stations": n,
            "n_failure_stations": sum(1 for r in rows if r["corrected_circular_mae_deg"] > r["baseline_circular_mae_deg"]),
            "network_mean_baseline_mae_deg": mean_base,
            "network_mean_corrected_mae_deg": mean_corr,
            "network_mean_mae_reduction_pct": mean_red,
        }
    if not by_hp:
        return {"error": "no_valid_hp"}
    best_label = max(by_hp, key=lambda k: by_hp[k]["network_mean_mae_reduction_pct"])
    return {"by_hp": by_hp, "best_hp_label": best_label, "best_summary": by_hp[best_label]}


def load_old_summary(model_dir, lead):
    paths = get_paths()
    path = paths.reports.diagnostics_dir / model_dir / f"wind_direction_lead{lead}.json"
    if not path.exists():
        return None
    with path.open() as f:
        data = json.load(f)
    return data.get("summary", {}).get("best_summary")


def fmt(v, suffix="%"):
    return "---" if v is None else f"{v:+.2f}{suffix}"


def fmt_delta(v):
    return "---" if v is None else f"{v:+.2f}pp"


def main():
    paths = get_paths()
    print("Loading canonical")
    df = load_canonical()
    stations = sorted(df["station_id"].unique())
    print(f"  {len(df):,} rows, {len(stations)} stations")
    print()
    print("Wind direction LOSO sweep v2 - CORRECTED reconstruction convention")
    print("  actual_residual_angle = degrees(arctan2(actual_sin, actual_cos))")
    print("  pred_residual_angle = degrees(arctan2(pred_sin, pred_cos))")
    print("  corrected_angle = mod(base_angle + pred_residual_angle, 360)")
    print()

    ridge_dir = paths.reports.diagnostics_dir / "ridge_loso_sweep_v2"
    hgb_dir = paths.reports.diagnostics_dir / "hgb_loso_sweep_v2"
    ridge_dir.mkdir(parents=True, exist_ok=True)
    hgb_dir.mkdir(parents=True, exist_ok=True)

    new_ridge_results = {}
    new_hgb_results = {}

    print("=== Ridge wind_direction sweep v2 ===")
    ridge_start = time.time()
    for lead in LEADS:
        t0 = time.time()
        results = ridge_sweep(df, lead, stations)
        elapsed = time.time() - t0
        summary = summarize(results, [f"alpha{a}" for a in ALPHAS])
        if "best_summary" in summary:
            b = summary["best_summary"]
            new_ridge_results[lead] = b["network_mean_mae_reduction_pct"]
            print(
                f"  lead={lead:>2}h  best={summary['best_hp_label']:<10}  "
                f"base_mae={b['network_mean_baseline_mae_deg']:>5.1f}deg  "
                f"corr_mae={b['network_mean_corrected_mae_deg']:>5.1f}deg  "
                f"red={b['network_mean_mae_reduction_pct']:>+6.2f}%  "
                f"fail={b['n_failure_stations']}/{b['n_stations']}  "
                f"time={elapsed:.1f}s"
            )
        path = ridge_dir / f"wind_direction_lead{lead}.json"
        with path.open("w") as f:
            json.dump({
                "target": TARGET, "lead": lead, "results": results, "summary": summary,
                "metric_type": "circular_mae_deg", "convention": "v2_angle_reconstruction",
            }, f, indent=2)
    print(f"  Ridge v2 total: {time.time() - ridge_start:.1f}s")
    print()

    print("=== HGB wind_direction sweep v2 ===")
    hgb_start = time.time()
    for lead in LEADS:
        t0 = time.time()
        results = hgb_sweep(df, lead, stations)
        elapsed = time.time() - t0
        summary = summarize(results, [c.label() for c in HGB_CONFIGS])
        if "best_summary" in summary:
            b = summary["best_summary"]
            new_hgb_results[lead] = b["network_mean_mae_reduction_pct"]
            print(
                f"  lead={lead:>2}h  best={summary['best_hp_label']:<10}  "
                f"base_mae={b['network_mean_baseline_mae_deg']:>5.1f}deg  "
                f"corr_mae={b['network_mean_corrected_mae_deg']:>5.1f}deg  "
                f"red={b['network_mean_mae_reduction_pct']:>+6.2f}%  "
                f"fail={b['n_failure_stations']}/{b['n_stations']}  "
                f"time={elapsed:.1f}s"
            )
        path = hgb_dir / f"wind_direction_lead{lead}.json"
        with path.open("w") as f:
            json.dump({
                "target": TARGET, "lead": lead, "results": results, "summary": summary,
                "metric_type": "circular_mae_deg", "convention": "v2_angle_reconstruction",
            }, f, indent=2)
    print(f"  HGB v2 total: {time.time() - hgb_start:.1f}s")
    print()

    print("=== Comparison: OLD (vector convention) vs NEW (angle convention) ===")
    print(
        f"  {'lead':>4}  {'old_ridge':>10}  {'new_ridge':>10}  {'ridge_delta':>12}  "
        f"{'old_hgb':>10}  {'new_hgb':>10}  {'hgb_delta':>10}"
    )
    for lead in LEADS:
        old_r = load_old_summary("ridge_loso_sweep", lead)
        old_h = load_old_summary("hgb_loso_sweep", lead)
        old_r_val = old_r["network_mean_mae_reduction_pct"] if old_r else None
        old_h_val = old_h["network_mean_mae_reduction_pct"] if old_h else None
        new_r_val = new_ridge_results.get(lead)
        new_h_val = new_hgb_results.get(lead)
        r_delta = (new_r_val - old_r_val) if (new_r_val is not None and old_r_val is not None) else None
        h_delta = (new_h_val - old_h_val) if (new_h_val is not None and old_h_val is not None) else None
        print(
            f"  {lead:>3}h  {fmt(old_r_val):>10}  {fmt(new_r_val):>10}  {fmt_delta(r_delta):>12}  "
            f"{fmt(old_h_val):>10}  {fmt(new_h_val):>10}  {fmt_delta(h_delta):>10}"
        )
    print()
    print("Per-target winner (under NEW convention):")
    for lead in LEADS:
        new_r = new_ridge_results.get(lead)
        new_h = new_hgb_results.get(lead)
        if new_r is None or new_h is None:
            continue
        winner = "HGB" if new_h >= new_r + 1.5 else "Ridge"
        print(f"  lead={lead:>2}h  Ridge={new_r:+.2f}%  HGB={new_h:+.2f}%  winner={winner}")


if __name__ == "__main__":
    main()
