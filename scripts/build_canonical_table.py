from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from postprocessing.features.circular import add_circular_wind_features
from postprocessing.features.lags import add_lag_features
from postprocessing.features.radiation import add_radiation_features
from postprocessing.features.rain import add_rain_features
from postprocessing.features.solar import add_solar_features, merge_daily_into_hourly
from postprocessing.features.time import add_time_features
from postprocessing.ingestion.station_registry import load_stations
from postprocessing.qc.gates import trainable_rows_per_target
from postprocessing.targets.derived import add_derived_features
from postprocessing.targets.pressure import add_pressure_residuals
from postprocessing.targets.rain import add_rain_targets
from postprocessing.targets.residuals import add_core_residuals
from postprocessing.targets.wind_direction import add_wind_direction_residual
from postprocessing.utils.paths import get_paths


@dataclass
class StageTiming:
    name: str
    seconds: float
    rows: int
    columns: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the canonical hourly training table from validated station data and Open-Meteo baselines.",
    )
    parser.add_argument(
        "--validated_parquet",
        default=None,
        help="Path to the validated station parquet. Defaults to the interim_stations_clean_dir.",
    )
    parser.add_argument(
        "--output_parquet",
        default=None,
        help="Where to write the canonical parquet. Defaults to data/processed/canonical_hourly_v1.parquet.",
    )
    parser.add_argument(
        "--limit_stations",
        nargs="*",
        default=None,
        help="Optional list of station_ids to limit processing to. For debugging.",
    )
    return parser.parse_args()


def _resolve_validated_parquet(arg_value: str | None) -> Path:
    if arg_value:
        return Path(arg_value)
    paths = get_paths()
    candidates = list(paths.data.interim_stations_clean_dir.glob("*.parquet"))
    if not candidates:
        raise FileNotFoundError(
            f"No parquet files found in {paths.data.interim_stations_clean_dir}. "
            "Run validate_station_table.py first."
        )
    if len(candidates) > 1:
        candidates = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _resolve_output_parquet(arg_value: str | None) -> Path:
    if arg_value:
        return Path(arg_value)
    return get_paths().data.processed_dir / "canonical_hourly_v1.parquet"


def _load_baseline_concatenated(suffix: str, station_ids: list[str]) -> pd.DataFrame:
    paths = get_paths()
    frames: list[pd.DataFrame] = []
    for sid in station_ids:
        path = paths.data.external_openmeteo_dir / f"{sid}_{suffix}.parquet"
        if not path.is_file():
            print(f"  WARNING: baseline file missing for {sid} ({suffix}): {path}", file=sys.stderr)
            continue
        frames.append(pd.read_parquet(path))
    if not frames:
        raise FileNotFoundError(
            f"No {suffix} baseline parquets found for any station in {paths.data.external_openmeteo_dir}"
        )
    return pd.concat(frames, ignore_index=True)


def _stamp(df: pd.DataFrame, stage: str, t0: float, log: list[StageTiming]) -> None:
    elapsed = time.perf_counter() - t0
    log.append(StageTiming(stage, elapsed, len(df), len(df.columns)))
    print(f"  {stage}: {elapsed:.2f}s, {len(df):,} rows, {len(df.columns)} cols", flush=True)


def main() -> int:
    args = parse_args()
    paths = get_paths()
    paths.data.processed_dir.mkdir(parents=True, exist_ok=True)
    paths.data.manifests_dir.mkdir(parents=True, exist_ok=True)

    log: list[StageTiming] = []
    overall_start = time.perf_counter()

    print("=== Loading inputs ===", flush=True)

    t0 = time.perf_counter()
    validated_path = _resolve_validated_parquet(args.validated_parquet)
    print(f"  Validated station parquet: {validated_path}")
    df = pd.read_parquet(validated_path)
    if args.limit_stations:
        keep = set(args.limit_stations)
        df = df[df["station_id"].isin(keep)].reset_index(drop=True)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    _stamp(df, "load_station", t0, log)

    t0 = time.perf_counter()
    stations = load_stations()
    if args.limit_stations:
        keep = set(args.limit_stations)
        stations = [s for s in stations if s.station_id in keep]
    elevation_map = {s.station_id: s.elevation_m for s in stations}
    df["elevation_m"] = df["station_id"].map(elevation_map)
    _stamp(df, "merge_elevation", t0, log)

    station_ids = sorted(df["station_id"].unique().tolist())

    t0 = time.perf_counter()
    hourly_baseline = _load_baseline_concatenated("hourly", station_ids)
    hourly_baseline["valid_time_utc"] = pd.to_datetime(
        hourly_baseline["valid_time_utc"], utc=True, errors="coerce"
    )
    _stamp(hourly_baseline, "load_hourly_baseline", t0, log)

    t0 = time.perf_counter()
    daily_baseline = _load_baseline_concatenated("daily", station_ids)
    daily_baseline["date_utc"] = pd.to_datetime(
        daily_baseline["date_utc"], utc=True, errors="coerce"
    )
    for col in ("base_sunrise_utc", "base_sunset_utc"):
        if col in daily_baseline.columns:
            daily_baseline[col] = pd.to_datetime(daily_baseline[col], utc=True, errors="coerce")
    _stamp(daily_baseline, "load_daily_baseline", t0, log)

    print("=== Merging baseline ===", flush=True)

    t0 = time.perf_counter()
    hourly_baseline = hourly_baseline.rename(columns={"valid_time_utc": "timestamp_utc"})
    df = df.merge(
        hourly_baseline,
        on=["station_id", "timestamp_utc"],
        how="left",
    )
    df = df.rename(columns={"timestamp_utc": "valid_time_utc"})
    _stamp(df, "merge_hourly_baseline", t0, log)

    t0 = time.perf_counter()
    df = merge_daily_into_hourly(df, daily_baseline)
    _stamp(df, "merge_daily_baseline", t0, log)

    print("=== Applying features ===", flush=True)

    feature_steps = [
        ("time_features", add_time_features),
        ("circular_wind_features", add_circular_wind_features),
        ("solar_features", add_solar_features),
        ("radiation_features", add_radiation_features),
        ("rain_features", add_rain_features),
        ("lag_features", add_lag_features),
    ]
    for name, fn in feature_steps:
        t0 = time.perf_counter()
        df = fn(df)
        _stamp(df, name, t0, log)

    print("=== Applying targets ===", flush=True)

    target_steps = [
        ("core_residuals", add_core_residuals),
        ("pressure_residuals", add_pressure_residuals),
        ("wind_direction_residual", add_wind_direction_residual),
        ("rain_targets", add_rain_targets),
        ("derived_features", add_derived_features),
    ]
    for name, fn in target_steps:
        t0 = time.perf_counter()
        df = fn(df)
        _stamp(df, name, t0, log)

    print("=== Writing output ===", flush=True)

    output_path = _resolve_output_parquet(args.output_parquet)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    df.to_parquet(output_path, engine="pyarrow", index=False)
    _stamp(df, "write_canonical", t0, log)

    trainable_counts = trainable_rows_per_target(df)

    overall_elapsed = time.perf_counter() - overall_start

    manifest = {
        "built_utc": datetime.now(timezone.utc).isoformat(),
        "validated_input_path": str(validated_path),
        "canonical_output_path": str(output_path),
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "station_count": int(df["station_id"].nunique()),
        "trainable_rows_per_target": trainable_counts,
        "elapsed_seconds_total": round(overall_elapsed, 3),
        "stages": [
            {
                "name": s.name,
                "seconds": round(s.seconds, 3),
                "rows": s.rows,
                "columns": s.columns,
            }
            for s in log
        ],
    }
    manifest_path = (
        paths.data.manifests_dir
        / f"canonical_build_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.json"
    )
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print()
    print(f"Manifest written: {manifest_path}")
    print(f"Canonical parquet: {output_path}")
    print(f"Total: {overall_elapsed:.2f}s, {len(df):,} rows, {len(df.columns)} columns")
    print(f"Trainable rows per target: {trainable_counts}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
