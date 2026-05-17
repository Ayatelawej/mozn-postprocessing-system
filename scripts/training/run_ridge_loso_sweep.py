from __future__ import annotations

import json
from pathlib import Path

from postprocessing.training.data_loader import load_canonical
from postprocessing.training.ridge_runner import fit_ridge, prepare_for_target
from postprocessing.utils.paths import get_paths


TARGET = "temperature"
LEAD = 1
ALPHAS = (0.01, 0.1, 1.0, 10.0, 100.0)


def main() -> None:
    paths = get_paths()
    print(f"Loading canonical")
    df = load_canonical()
    stations = sorted(df["station_id"].unique())
    print(f"  Loaded: {len(df):,} rows, {len(stations)} stations")
    print()
    print(f"Preparing frame for target={TARGET}, lead={LEAD}")
    framed = prepare_for_target(df, TARGET, LEAD)
    print(f"  Framed: {len(framed):,} rows")
    print()
    print(f"Sweeping {len(stations)} stations x {len(ALPHAS)} alphas = {len(stations) * len(ALPHAS)} fits")
    print()

    results = []
    for i, station in enumerate(stations):
        for alpha in ALPHAS:
            try:
                result, _ = fit_ridge(framed, TARGET, LEAD, station, alpha=alpha)
                results.append(result.as_dict())
            except Exception as exc:
                results.append({
                    "target": TARGET,
                    "lead": LEAD,
                    "holdout_station": station,
                    "alpha": alpha,
                    "error": str(exc),
                })
        print(f"  [{i+1}/{len(stations)}] {station} done")
    print()

    valid = [r for r in results if "error" not in r]

    print("=== Network-mean by alpha ===")
    print(f"  {'alpha':>8}  {'base_mae':>10}  {'corr_mae':>10}  {'mae_red%':>10}  {'base_bias':>10}  {'corr_bias':>10}  {'bias_corr%':>10}")
    alpha_summary = {}
    for alpha in ALPHAS:
        rows = [r for r in valid if r["alpha"] == alpha]
        if not rows:
            continue
        n = len(rows)
        mean_base_mae = sum(r["baseline_mae"] for r in rows) / n
        mean_corr_mae = sum(r["corrected_mae"] for r in rows) / n
        mean_mae_red = sum(r["mae_reduction_pct"] for r in rows) / n
        mean_base_bias = sum(r["baseline_bias"] for r in rows) / n
        mean_corr_bias = sum(r["corrected_bias"] for r in rows) / n
        mean_bias_corr = sum(r["bias_correction_pct"] for r in rows) / n
        alpha_summary[alpha] = mean_mae_red
        print(f"  {alpha:>8.2f}  {mean_base_mae:>10.4f}  {mean_corr_mae:>10.4f}  {mean_mae_red:>+10.2f}  {mean_base_bias:>+10.4f}  {mean_corr_bias:>+10.4f}  {mean_bias_corr:>+10.2f}")
    print()

    if alpha_summary:
        best_alpha = max(alpha_summary, key=alpha_summary.get)
        print(f"Best alpha by mean MAE reduction: {best_alpha}")
        print()

        print(f"=== Per-station results at alpha={best_alpha} ===")
        print(f"  {'station':>11}  {'base_mae':>10}  {'corr_mae':>10}  {'mae_red%':>10}  {'base_bias':>10}  {'corr_bias':>10}  {'bias_corr%':>10}")
        best_rows = sorted(
            [r for r in valid if r["alpha"] == best_alpha],
            key=lambda r: -r["mae_reduction_pct"],
        )
        for r in best_rows:
            print(f"  {r['holdout_station']:>11}  {r['baseline_mae']:>10.4f}  {r['corrected_mae']:>10.4f}  {r['mae_reduction_pct']:>+10.2f}  {r['baseline_bias']:>+10.4f}  {r['corrected_bias']:>+10.4f}  {r['bias_correction_pct']:>+10.2f}")
        print()

        failures = [r for r in best_rows if r["corrected_mae"] > r["baseline_mae"]]
        print(f"=== Stations where Ridge makes MAE worse at alpha={best_alpha} ===")
        if not failures:
            print("  None")
        else:
            for r in failures:
                delta = r["corrected_mae"] - r["baseline_mae"]
                print(f"  {r['holdout_station']:>11}  baseline_mae={r['baseline_mae']:.4f}  corrected_mae={r['corrected_mae']:.4f}  delta={delta:+.4f}  bias_corr={r['bias_correction_pct']:+.2f}%")
        print()

    output_path = paths.reports.diagnostics_dir / "ridge_loso_sweep" / f"{TARGET}_lead{LEAD}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        json.dump({
            "target": TARGET,
            "lead": LEAD,
            "alphas": list(ALPHAS),
            "stations": stations,
            "results": results,
        }, f, indent=2)
    print(f"Results saved to {output_path}")


if __name__ == "__main__":
    main()
