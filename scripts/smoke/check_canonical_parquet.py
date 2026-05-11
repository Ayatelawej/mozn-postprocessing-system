from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

EXPECTED_FEATURES = {
    "hour_sin",
    "hour_cos",
    "is_day_flag",
    "station_wind_direction_sin",
    "station_wind_direction_cos",
    "base_wind_direction_sin",
    "base_wind_direction_cos",
    "station_wind_u_kmh",
    "station_wind_v_kmh",
    "base_wind_u_kmh",
    "base_wind_v_kmh",
    "minutes_from_sunrise",
    "solar_progress_0_1",
    "solar_centered",
    "clipped_shortwave_wm2",
    "cloud_attenuation_factor",
    "sunshine_fraction",
    "uv_proxy",
    "solar_to_clear_sky_ratio",
    "station_rain_event",
    "base_rain_event",
    "station_rain_rolling_3h_mm",
    "station_rain_rolling_6h_mm",
    "base_rain_rolling_3h_mm",
    "base_rain_rolling_6h_mm",
}

EXPECTED_TARGETS = {
    "temperature_residual_c",
    "relative_humidity_residual_pct",
    "dew_point_residual_c",
    "wind_speed_residual_kmh",
    "wind_gust_residual_kmh",
    "pressure_residual_max_hpa",
    "pressure_residual_min_hpa",
    "pressure_residual_avg_hpa",
    "winddir_residual_deg",
    "winddir_residual_sin",
    "winddir_residual_cos",
    "rain_occurrence",
    "rain_amount_log1p",
    "rain_amount_mm",
    "heat_index_c",
    "wind_chill_c",
}

EXPECTED_GATES = {
    "gate_temperature_ready",
    "gate_relative_humidity_ready",
    "gate_dew_point_ready",
    "gate_wind_speed_ready",
    "gate_wind_gust_ready",
    "gate_pressure_ready",
    "gate_uv_ready",
    "gate_wind_direction_ready",
    "gate_rain_occurrence_ready",
    "gate_rain_amount_ready",
}

EXPECTED_LAG_VARIABLES = (
    "temperature_c",
    "relative_humidity_pct",
    "dew_point_c",
    "wind_speed_kmh",
    "pressure_max_hpa",
    "rain_total_mm",
)
LAG_OFFSETS = (1, 2, 3, 6, 12, 24)
ROLL_WINDOWS = (3, 6, 12, 24)


def main() -> int:
    parquet_path = Path("data/processed/canonical_hourly_v1.parquet")
    if not parquet_path.is_file():
        print(f"FAIL: canonical parquet missing at {parquet_path}", file=sys.stderr)
        return 1

    df = pd.read_parquet(parquet_path)
    failures: list[str] = []

    print("=== Shape and basic stats ===")
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Stations: {df['station_id'].nunique()}")
    print(f"  Time range: {df['valid_time_utc'].min()} to {df['valid_time_utc'].max()}")
    print()

    print("=== Required columns ===")
    missing_features = EXPECTED_FEATURES - set(df.columns)
    missing_targets = EXPECTED_TARGETS - set(df.columns)
    missing_gates = EXPECTED_GATES - set(df.columns)
    if missing_features:
        failures.append(f"Missing feature columns: {sorted(missing_features)}")
    if missing_targets:
        failures.append(f"Missing target columns: {sorted(missing_targets)}")
    if missing_gates:
        failures.append(f"Missing gate columns: {sorted(missing_gates)}")
    print(f"  Features present: {len(EXPECTED_FEATURES) - len(missing_features)}/{len(EXPECTED_FEATURES)}")
    print(f"  Targets present: {len(EXPECTED_TARGETS) - len(missing_targets)}/{len(EXPECTED_TARGETS)}")
    print(f"  Gates present: {len(EXPECTED_GATES) - len(missing_gates)}/{len(EXPECTED_GATES)}")
    print()

    print("=== Lag and rolling columns ===")
    for var in EXPECTED_LAG_VARIABLES:
        for h in LAG_OFFSETS:
            col = f"{var}_lag_{h}h"
            if col not in df.columns:
                failures.append(f"Missing lag column: {col}")
        for w in ROLL_WINDOWS:
            for stat in ("mean", "std"):
                col = f"{var}_roll_{stat}_{w}h"
                if col not in df.columns:
                    failures.append(f"Missing rolling column: {col}")
    print(f"  Expected lag columns:    {len(EXPECTED_LAG_VARIABLES) * len(LAG_OFFSETS)}")
    print(f"  Expected rolling columns: {len(EXPECTED_LAG_VARIABLES) * len(ROLL_WINDOWS) * 2}")
    print()

    print("=== Target column non-null counts ===")
    for col in sorted(EXPECTED_TARGETS):
        if col in df.columns:
            non_null = int(df[col].notna().sum())
            print(f"  {col:42s} {non_null:>7,}")
            if non_null == 0:
                failures.append(f"Target column has zero non-null values: {col}")
    print()

    print("=== Range sanity (selected columns) ===")
    range_checks = [
        ("temperature_c", -10, 60),
        ("relative_humidity_pct", 0, 110),
        ("wind_speed_kmh", 0, 200),
        ("pressure_max_hpa", 850, 1100),
        ("rain_total_mm", 0, 10000),
        ("hour_sin", -1.001, 1.001),
        ("hour_cos", -1.001, 1.001),
        ("temperature_residual_c", -30, 30),
        ("pressure_residual_avg_hpa", -100, 100),
        ("rain_occurrence", 0, 1),
        ("solar_progress_0_1", -0.001, 1.001),
        ("sunshine_fraction", -0.001, 1.001),
    ]
    for col, lo, hi in range_checks:
        if col not in df.columns:
            continue
        s = df[col].dropna()
        if len(s) == 0:
            print(f"  {col:42s} all-null")
            continue
        actual_min, actual_max = s.min(), s.max()
        ok = "PASS" if (actual_min >= lo and actual_max <= hi) else "FAIL"
        print(f"  {col:42s} [{actual_min:>10.3f}, {actual_max:>10.3f}]  expected [{lo}, {hi}]  {ok}")
        if ok == "FAIL":
            failures.append(f"{col} out of expected range: actual=[{actual_min}, {actual_max}], expected=[{lo}, {hi}]")
    print()

    print("=== Lag NaN pattern ===")
    a_station = sorted(df["station_id"].unique())[0]
    sub = df[df["station_id"] == a_station].sort_values("valid_time_utc").reset_index(drop=True)
    if "temperature_c_lag_1h" in sub.columns and len(sub) > 0:
        first_nan = bool(pd.isna(sub.iloc[0]["temperature_c_lag_1h"]))
        second_not_nan = bool(not pd.isna(sub.iloc[1]["temperature_c_lag_1h"])) if len(sub) > 1 else False
        print(f"  Station {a_station}: first row lag_1h is NaN: {first_nan}")
        print(f"  Station {a_station}: second row lag_1h is non-NaN: {second_not_nan}")
        if not first_nan:
            failures.append(f"First row of station {a_station} has non-NaN lag_1h, expected NaN")
        if not second_not_nan:
            failures.append(f"Second row of station {a_station} has NaN lag_1h, expected non-NaN")
    if "temperature_c_lag_24h" in sub.columns and len(sub) > 24:
        for i in range(24):
            if not pd.isna(sub.iloc[i]["temperature_c_lag_24h"]):
                failures.append(f"Station {a_station} row {i} has non-NaN lag_24h, expected NaN (need 24h history)")
                break
        else:
            print(f"  Station {a_station}: rows 0-23 lag_24h all NaN: True")
    print()

    print("=== Trainable counts per target (with overrides) ===")
    from postprocessing.qc.gates import trainable_rows_per_target

    counts_with = trainable_rows_per_target(df, apply_overrides=True)
    counts_without = trainable_rows_per_target(df, apply_overrides=False)
    for target in sorted(counts_with.keys()):
        with_n = counts_with[target]
        without_n = counts_without[target]
        delta = without_n - with_n
        marker = f"  (-{delta:,} via override)" if delta else ""
        print(f"  {target:24s} {with_n:>7,}{marker}")
    print()

    print("=== Result ===")
    if failures:
        print(f"FAIL: {len(failures)} issues found")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("PASS: all smoke tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
