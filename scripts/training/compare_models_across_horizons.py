from __future__ import annotations

import json

from postprocessing.utils.paths import get_paths


TARGETS: tuple[str, ...] = (
    "temperature", "relative_humidity", "dew_point",
    "wind_speed", "wind_gust", "pressure",
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
            if "best_summary" not in summary:
                continue
            b = summary["best_summary"]
            out[(target, lead)] = {
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
    print(f"=== {label}: network mean MAE reduction by horizon ===")
    header = f"  {'target':>20}  " + "  ".join(f"{f'L{lead}':>9}" for lead in LEADS)
    print(header)
    for target in TARGETS:
        row = [f"  {target:>20}"]
        for lead in LEADS:
            s = summaries.get((target, lead))
            row.append(f"{'---':>9}" if s is None else f"{s['mean_mae_red']:>+8.2f}%")
        print("  ".join(row))
    print()


def print_baseline_table(label: str, summaries: dict) -> None:
    print(f"=== {label}: network mean baseline MAE (Open-Meteo alone) ===")
    header = f"  {'target':>20}  " + "  ".join(f"{f'L{lead}':>9}" for lead in LEADS)
    print(header)
    for target in TARGETS:
        row = [f"  {target:>20}"]
        for lead in LEADS:
            s = summaries.get((target, lead))
            row.append(f"{'---':>9}" if s is None else f"{s['mean_baseline_mae']:>9.3f}")
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

    print(f"=== Per-(target, lead) winner (HGB if mean MAE red >= Ridge + {WIN_THRESHOLD_PP}pp, else Ridge) ===")
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
            else:
                w = "HGB" if h["mean_mae_red"] >= r["mean_mae_red"] + WIN_THRESHOLD_PP else "Ridge"
            per_pair_winner[(target, lead)] = w
            row.append(f"{w:>9}")
        print("  ".join(row))
    print()

    print("=== Per-target recommended model (avg across all available leads) ===")
    print(f"  {'target':>20}  {'avg_ridge':>10}  {'avg_hgb':>10}  {'delta':>9}  {'winner':>8}  {'n_leads':>8}")
    selection = {}
    for target in TARGETS:
        r_vals = [ridge[(target, lead)]["mean_mae_red"] for lead in LEADS if (target, lead) in ridge]
        h_vals = [hgb[(target, lead)]["mean_mae_red"] for lead in LEADS if (target, lead) in hgb]
        if not r_vals or not h_vals:
            print(f"  {target:>20}  incomplete data")
            continue
        n_leads = min(len(r_vals), len(h_vals))
        avg_r = sum(r_vals) / len(r_vals)
        avg_h = sum(h_vals) / len(h_vals)
        delta = avg_h - avg_r
        winner = "HGB" if delta >= WIN_THRESHOLD_PP else "Ridge"
        selection[target] = {
            "winner": winner,
            "avg_ridge_mae_red": avg_r,
            "avg_hgb_mae_red": avg_h,
            "delta_pp": delta,
            "n_leads": n_leads,
        }
        print(
            f"  {target:>20}  {avg_r:>+9.2f}%  {avg_h:>+9.2f}%  {delta:>+8.2f}  {winner:>8}  {n_leads:>8}"
        )
    print()

    print("=== Per-target horizon-stability check (max - min across leads) ===")
    print(f"  {'target':>20}  {'ridge_range':>12}  {'hgb_range':>12}  {'notes':>20}")
    for target in TARGETS:
        r_vals = [ridge[(target, lead)]["mean_mae_red"] for lead in LEADS if (target, lead) in ridge]
        h_vals = [hgb[(target, lead)]["mean_mae_red"] for lead in LEADS if (target, lead) in hgb]
        if not r_vals or not h_vals:
            continue
        r_range = max(r_vals) - min(r_vals)
        h_range = max(h_vals) - min(h_vals)
        note = ""
        if r_range > 30 or h_range > 30:
            note = "STEEP_DEGRADATION"
        elif min(r_vals) < 10 or min(h_vals) < 10:
            note = "WEAK_AT_LONG_LEAD"
        print(f"  {target:>20}  {r_range:>11.2f}pp  {h_range:>11.2f}pp  {note:>20}")
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
