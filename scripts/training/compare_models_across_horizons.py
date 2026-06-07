from __future__ import annotations

import json

from postprocessing.utils.paths import get_paths


TARGETS: tuple[str, ...] = (
    "temperature", "relative_humidity", "dew_point",
    "wind_speed", "wind_gust", "pressure",
    "uv", "wind_direction",
)
LEADS: tuple[int, ...] = (1, 6, 24, 48, 72)
WIN_THRESHOLD_PP: float = 1.5


def load_summaries(model_dir: str) -> dict:
    paths = get_paths()
    base = paths.reports.diagnostics_dir / model_dir
    out = {}
    for target in TARGETS:
        for lead in LEADS:
            path = base / f"{target}_lead{lead}.json"
            if not path.exists():
                continue
            with path.open() as f:
                data = json.load(f)
            summary = data.get("summary", {})
            metric_type = data.get("metric_type", "baseline_relative")
            if "best_summary" not in summary:
                continue
            b = summary["best_summary"]
            if metric_type == "absolute_uv_index":
                out[(target, lead)] = {
                    "metric_type": "absolute",
                    "absolute_mae": b["network_mean_absolute_mae"],
                    "absolute_rmse": b["network_mean_absolute_rmse"],
                    "absolute_bias": b["network_mean_absolute_bias"],
                    "target_mean": b.get("network_mean_target_mean"),
                    "target_std": b.get("network_mean_target_std"),
                    "n_stations": b["n_stations"],
                    "best_hp": str(summary.get("best_alpha", summary.get("best_config_label", "?"))),
                }
            elif metric_type == "circular_mae_deg":
                out[(target, lead)] = {
                    "metric_type": "circular",
                    "baseline_mae_deg": b["network_mean_baseline_mae_deg"],
                    "corrected_mae_deg": b["network_mean_corrected_mae_deg"],
                    "mean_mae_red": b["network_mean_mae_reduction_pct"],
                    "baseline_mae_filt_deg": b["network_mean_baseline_mae_filtered_deg"],
                    "corrected_mae_filt_deg": b["network_mean_corrected_mae_filtered_deg"],
                    "mae_red_filt": b["network_mean_mae_reduction_filtered_pct"],
                    "n_failures": b["n_failure_stations"],
                    "n_stations": b["n_stations"],
                    "best_hp": summary.get("best_hp_label", "?"),
                }
            else:
                out[(target, lead)] = {
                    "metric_type": "relative",
                    "mean_mae_red": b["network_mean_mae_reduction_pct"],
                    "median_mae_red": b["network_median_mae_reduction_pct"],
                    "mean_baseline_mae": b["network_mean_baseline_mae"],
                    "mean_corrected_mae": b["network_mean_corrected_mae"],
                    "mean_bias_corr": b.get("network_bias_correction_pct"),
                    "bias_meaningful": b.get("bias_meaningful", True),
                    "n_failures": b["n_failure_stations"],
                    "n_stations": b["n_stations"],
                    "best_hp": str(summary.get("best_alpha", summary.get("best_config_label", "?"))),
                }
    return out


def print_horizon_table(label: str, summaries: dict) -> None:
    print(f"=== {label}: per-target horizon view ===")
    for target in TARGETS:
        row_data = []
        any_loaded = False
        for lead in LEADS:
            s = summaries.get((target, lead))
            if s is None:
                row_data.append("---")
                continue
            any_loaded = True
            if s["metric_type"] == "absolute":
                row_data.append(f"abs_mae={s['absolute_mae']:.3f}")
            elif s["metric_type"] == "circular":
                row_data.append(f"{s['mean_mae_red']:+.2f}% deg")
            else:
                row_data.append(f"{s['mean_mae_red']:+.2f}%")
        if not any_loaded:
            continue
        cells = "  ".join(f"{cell:>14}" for cell in row_data)
        print(f"  {target:>20}  {cells}")
    print()


def print_baseline_table(label: str, summaries: dict) -> None:
    print(f"=== {label}: network mean baseline MAE (Open-Meteo alone) ===")
    header = f"  {'target':>20}  " + "  ".join(f"{f'L{lead}':>9}" for lead in LEADS)
    print(header)
    for target in TARGETS:
        row = [f"  {target:>20}"]
        for lead in LEADS:
            s = summaries.get((target, lead))
            if s is None or s["metric_type"] == "absolute":
                row.append(f"{'---':>9}")
            elif s["metric_type"] == "circular":
                row.append(f"{s['baseline_mae_deg']:>9.3f}")
            else:
                row.append(f"{s['mean_baseline_mae']:>9.3f}")
        print("  ".join(row))
    print()


def main() -> None:
    paths = get_paths()
    ridge = load_summaries("ridge_loso_sweep")
    hgb = load_summaries("hgb_loso_sweep")

    print(f"Loaded Ridge entries: {len(ridge)} / {len(TARGETS) * len(LEADS)}")
    print(f"Loaded HGB entries:   {len(hgb)} / {len(TARGETS) * len(LEADS)}")
    print()

    print_baseline_table("Baseline", ridge if ridge else hgb)
    print_horizon_table("Ridge", ridge)
    print_horizon_table("HGB", hgb)

    print(f"=== Per-(target, lead) winner ===")
    header = f"  {'target':>20}  " + "  ".join(f"{f'L{lead}':>9}" for lead in LEADS)
    print(header)
    per_pair_winner = {}
    for target in TARGETS:
        row = [f"  {target:>20}"]
        for lead in LEADS:
            r = ridge.get((target, lead))
            h = hgb.get((target, lead))
            if r is None and h is None:
                row.append(f"{'---':>9}")
                continue
            if r is None:
                w = "HGB"
            elif h is None:
                w = "Ridge"
            elif r["metric_type"] == "absolute":
                w = "HGB" if r["absolute_mae"] - h["absolute_mae"] > 0.001 else "Ridge"
            else:
                w = "HGB" if h["mean_mae_red"] >= r["mean_mae_red"] + WIN_THRESHOLD_PP else "Ridge"
            per_pair_winner[(target, lead)] = w
            row.append(f"{w:>9}")
        print("  ".join(row))
    print()

    print("=== Per-target recommended model ===")
    print(f"  {'target':>20}  {'metric':>10}  {'ridge_avg':>12}  {'hgb_avg':>12}  {'delta':>9}  {'winner':>8}  {'n_leads':>8}")
    selection = {}
    for target in TARGETS:
        r_entries = [ridge[(target, lead)] for lead in LEADS if (target, lead) in ridge]
        h_entries = [hgb[(target, lead)] for lead in LEADS if (target, lead) in hgb]
        if not r_entries or not h_entries:
            print(f"  {target:>20}  incomplete data")
            continue
        n_leads = min(len(r_entries), len(h_entries))
        metric_type = r_entries[0]["metric_type"]
        if metric_type == "absolute":
            avg_r = sum(e["absolute_mae"] for e in r_entries) / len(r_entries)
            avg_h = sum(e["absolute_mae"] for e in h_entries) / len(h_entries)
            delta = avg_r - avg_h
            winner = "HGB" if delta > 0.001 else "Ridge"
            metric_label = "abs_mae"
            r_str = f"{avg_r:>11.3f}"
            h_str = f"{avg_h:>11.3f}"
            delta_str = f"{delta:>+8.3f}"
        elif metric_type == "circular":
            avg_r = sum(e["mean_mae_red"] for e in r_entries) / len(r_entries)
            avg_h = sum(e["mean_mae_red"] for e in h_entries) / len(h_entries)
            delta = avg_h - avg_r
            winner = "HGB" if delta >= WIN_THRESHOLD_PP else "Ridge"
            metric_label = "circ_red%"
            r_str = f"{avg_r:>+10.2f}%"
            h_str = f"{avg_h:>+10.2f}%"
            delta_str = f"{delta:>+8.2f}"
        else:
            avg_r = sum(e["mean_mae_red"] for e in r_entries) / len(r_entries)
            avg_h = sum(e["mean_mae_red"] for e in h_entries) / len(h_entries)
            delta = avg_h - avg_r
            winner = "HGB" if delta >= WIN_THRESHOLD_PP else "Ridge"
            metric_label = "mae_red%"
            r_str = f"{avg_r:>+10.2f}%"
            h_str = f"{avg_h:>+10.2f}%"
            delta_str = f"{delta:>+8.2f}"
        selection[target] = {
            "metric": metric_label,
            "winner": winner,
            "avg_ridge": avg_r,
            "avg_hgb": avg_h,
            "delta": delta,
            "n_leads": n_leads,
        }
        print(
            f"  {target:>20}  {metric_label:>10}  {r_str}  {h_str}  {delta_str}  {winner:>8}  {n_leads:>8}"
        )
    print()

    out_path = paths.reports.diagnostics_dir / "model_selection_lead1_to_72.json"
    with out_path.open("w") as f:
        json.dump({
            "leads": list(LEADS),
            "win_threshold_pp": WIN_THRESHOLD_PP,
            "ridge_by_lead": {f"{t}_lead{lead}": v for (t, lead), v in ridge.items()},
            "hgb_by_lead": {f"{t}_lead{lead}": v for (t, lead), v in hgb.items()},
            "per_pair_winner": {f"{t}_lead{lead}": w for (t, lead), w in per_pair_winner.items()},
            "per_target_selection": selection,
        }, f, indent=2)
    print(f"Saved selection: {out_path}")


if __name__ == "__main__":
    main()
