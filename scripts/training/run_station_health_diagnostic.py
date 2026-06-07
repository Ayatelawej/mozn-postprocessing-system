from __future__ import annotations

import json

import numpy as np
import pandas as pd

from postprocessing.qc.gates import filter_to_trainable
from postprocessing.training.data_loader import (
    TARGET_TO_TARGET_COLUMNS,
    load_canonical,
)
from postprocessing.utils.paths import get_paths


STATIONS_OF_INTEREST: tuple[str, ...] = ("IALWAH18", "IBARAS3", "INUQAT10", "IJABAL13")
TARGETS: tuple[str, ...] = (
    "temperature", "relative_humidity", "dew_point",
    "wind_speed", "wind_gust", "pressure",
)


def compute_target_stats(df: pd.DataFrame, target: str) -> dict:
    residual_col = TARGET_TO_TARGET_COLUMNS[target][0]
    filtered = filter_to_trainable(df, target)
    if filtered.empty:
        return {}
    result = {}
    for sid, group in filtered.groupby("station_id"):
        sorted_group = group.sort_values("valid_time_utc")
        res = sorted_group[residual_col].dropna()
        if len(res) < 24:
            continue
        arr = res.to_numpy()
        n = len(arr)
        lag1 = float(np.corrcoef(arr[:-1], arr[1:])[0, 1]) if n >= 2 else float("nan")
        half = n // 2
        first_half = float(np.mean(arr[:half]))
        second_half = float(np.mean(arr[half:]))
        mean = float(np.mean(arr))
        std = float(np.std(arr, ddof=0))
        result[sid] = {
            "n_trainable": int(n),
            "residual_mean": mean,
            "residual_std": std,
            "residual_mae": float(np.mean(np.abs(arr))),
            "residual_max_abs": float(np.max(np.abs(arr))),
            "lag1_autocorr": lag1,
            "first_half_mean": first_half,
            "second_half_mean": second_half,
            "drift": second_half - first_half,
            "n_3sigma_outliers": int(np.sum(np.abs(arr - mean) / max(std, 1e-9) > 3)),
        }
    return result


def network_medians(per_station_stats: dict) -> dict:
    if not per_station_stats:
        return {}
    keys = next(iter(per_station_stats.values())).keys()
    medians = {}
    for k in keys:
        values = []
        for s in per_station_stats.values():
            v = s.get(k)
            if v is not None and not (isinstance(v, float) and np.isnan(v)):
                values.append(v)
        if values:
            medians[k] = float(np.median(values))
    return medians


def main() -> None:
    paths = get_paths()
    print("Loading canonical")
    df = load_canonical()
    print(f"  Loaded: {len(df):,} rows, {df['station_id'].nunique()} stations")
    print()

    all_stats = {}
    all_medians = {}
    for target in TARGETS:
        stats = compute_target_stats(df, target)
        all_stats[target] = stats
        all_medians[target] = network_medians(stats)

    meta_df = (
        df.groupby("station_id")
        .agg(
            elevation_m=("elevation_m", "first"),
            station_latitude=("station_latitude", "first"),
            station_longitude=("station_longitude", "first"),
            raw_rows=("station_id", "count"),
        )
    )

    output = {}
    for sid in STATIONS_OF_INTEREST:
        if sid not in meta_df.index:
            print(f"=== {sid} === NOT FOUND in canonical")
            print()
            continue
        row = meta_df.loc[sid]
        meta = {
            "elevation_m": float(row["elevation_m"]),
            "station_latitude": float(row["station_latitude"]),
            "station_longitude": float(row["station_longitude"]),
            "raw_rows": int(row["raw_rows"]),
        }
        print(f"=== {sid} ===")
        print(f"  Elevation: {meta['elevation_m']:.0f} m")
        print(f"  Coords:    ({meta['station_latitude']:.3f}, {meta['station_longitude']:.3f})")
        print(f"  Raw rows:  {meta['raw_rows']:,}")
        print()
        print(f"  {'target':>18}  {'n_train':>7}  {'mean':>8}  {'std':>8}  {'autocor':>8}  {'drift':>8}  {'max|r|':>9}  {'3sig':>5}  flags")
        sid_summary = {"meta": meta, "targets": {}}
        for target in TARGETS:
            s = all_stats[target].get(sid)
            if s is None:
                print(f"  {target:>18}  ---- excluded by override or insufficient trainable rows ----")
                sid_summary["targets"][target] = None
                continue
            net = all_medians[target]
            flags = []
            if net.get("residual_std", 0) > 0 and s["residual_std"] > 2 * net["residual_std"]:
                flags.append("HIGH_STD")
            if not np.isnan(s["lag1_autocorr"]) and abs(s["lag1_autocorr"]) < 0.3:
                flags.append("LOW_AUTOCORR")
            if abs(s["drift"]) > 0.5 * max(s["residual_std"], 0.1):
                flags.append("DRIFT")
            if s["n_3sigma_outliers"] > 0.01 * s["n_trainable"]:
                flags.append("OUTLIERS")
            flag_str = ",".join(flags) if flags else "-"
            print(
                f"  {target:>18}  {s['n_trainable']:>7}  "
                f"{s['residual_mean']:>+8.3f}  {s['residual_std']:>8.3f}  "
                f"{s['lag1_autocorr']:>+8.3f}  {s['drift']:>+8.3f}  "
                f"{s['residual_max_abs']:>9.3f}  {s['n_3sigma_outliers']:>5d}  {flag_str}"
            )
            sid_summary["targets"][target] = {**s, "flags": flags}
        print()
        output[sid] = sid_summary

    print("=== Network medians (across all 26 stations, per target) ===")
    print(f"  {'target':>18}  {'mean':>8}  {'std':>8}  {'autocor':>8}  {'drift':>8}")
    for target in TARGETS:
        net = all_medians[target]
        print(
            f"  {target:>18}  "
            f"{net.get('residual_mean', float('nan')):>+8.3f}  "
            f"{net.get('residual_std', float('nan')):>8.3f}  "
            f"{net.get('lag1_autocorr', float('nan')):>+8.3f}  "
            f"{net.get('drift', float('nan')):>+8.3f}"
        )
    print()

    print("Flag legend:")
    print("  HIGH_STD     residual std > 2x network median (station noisier than typical)")
    print("  LOW_AUTOCORR |lag-1 autocorr| < 0.3 (residual is unpredictable, not just biased)")
    print("  DRIFT        |first-half mean - second-half mean| > 0.5 x std (calibration shift)")
    print("  OUTLIERS     > 1% of rows are 3-sigma outliers (sensor spikes / surges)")
    print()

    output_path = paths.reports.diagnostics_dir / "station_health" / "task5_5_diagnostic.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        json.dump({
            "stations_of_interest": list(STATIONS_OF_INTEREST),
            "targets": list(TARGETS),
            "stations": output,
            "network_medians": all_medians,
        }, f, indent=2, default=str)
    print(f"Saved: {output_path}")


if __name__ == "__main__":
    main()
