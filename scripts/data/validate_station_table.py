from pathlib import Path
import argparse
import json
import pandas as pd
import yaml

parser = argparse.ArgumentParser()
parser.add_argument("--input_parquet", required=True)
parser.add_argument("--station_registry_csv", required=True)
parser.add_argument("--rules_yaml", required=True)
parser.add_argument("--output_validated_parquet", required=True)
parser.add_argument("--output_summary_json", required=True)
parser.add_argument("--output_station_csv", required=True)
parser.add_argument("--output_target_csv", required=True)
parser.add_argument("--output_variable_csv", required=True)
args = parser.parse_args()

with open(args.rules_yaml, "r", encoding="utf-8") as f:
    rules = yaml.safe_load(f)

df = pd.read_parquet(args.input_parquet)
station_registry = pd.read_csv(args.station_registry_csv)

df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
df["data_present"] = pd.to_numeric(df["data_present"], errors="coerce")

for col in ["install_date", "pooled_first_timestamp", "pooled_last_timestamp"]:
    if col in station_registry.columns:
        station_registry[col] = pd.to_datetime(station_registry[col], utc=True, errors="coerce")

activity_threshold = rules["activity_reference"]["data_present_positive_threshold"]

active_df = df[df["data_present"].fillna(0) > activity_threshold].copy()

station_activity = active_df.groupby("station_id").agg(
    first_present_timestamp=("timestamp_utc", "min"),
    last_present_timestamp=("timestamp_utc", "max"),
    active_rows=("station_id", "size")
).reset_index()

station_total_rows = df.groupby("station_id").agg(
    total_rows=("station_id", "size")
).reset_index()

station_activity = station_total_rows.merge(station_activity, on="station_id", how="left")

station_activity["warmup_days"] = rules["warmup"]["default_days"]
overrides = rules["warmup"].get("station_override_days", {})
station_activity["warmup_days"] = station_activity["station_id"].map(overrides).fillna(station_activity["warmup_days"])
station_activity["warmup_end_timestamp"] = station_activity["first_present_timestamp"] + pd.to_timedelta(station_activity["warmup_days"], unit="D")

df = df.merge(
    station_activity[["station_id", "first_present_timestamp", "last_present_timestamp", "active_rows", "total_rows", "warmup_days", "warmup_end_timestamp"]],
    on="station_id",
    how="left"
)

df["flag_missing_station_id"] = df["station_id"].isna()
df["flag_missing_timestamp_utc"] = df["timestamp_utc"].isna()
df["flag_station_never_active"] = df["first_present_timestamp"].isna()
df["flag_data_absent"] = df["data_present"].fillna(0) <= activity_threshold
df["flag_before_first_present"] = df["first_present_timestamp"].notna() & (df["timestamp_utc"] < df["first_present_timestamp"])
df["flag_after_last_present"] = df["last_present_timestamp"].notna() & (df["timestamp_utc"] > df["last_present_timestamp"])
df["flag_warmup"] = df["warmup_end_timestamp"].notna() & (df["timestamp_utc"] < df["warmup_end_timestamp"]) & ~df["flag_before_first_present"]

variable_rows = []

for variable_name, bounds in rules["hard_range_rules"].items():
    if variable_name in df.columns:
        numeric_series = pd.to_numeric(df[variable_name], errors="coerce")
        df[variable_name] = numeric_series

        out_of_range = pd.Series(False, index=df.index)

        if "min" in bounds and bounds["min"] is not None:
            out_of_range = out_of_range | (numeric_series.notna() & (numeric_series < bounds["min"]))

        if "max" in bounds and bounds["max"] is not None:
            out_of_range = out_of_range | (numeric_series.notna() & (numeric_series > bounds["max"]))

        flag_col = f"flag_{variable_name}_out_of_range"
        df[flag_col] = out_of_range

        advisory_count = None
        advisory_pct = None
        advisory_min = None
        advisory_max = None
        advisory_source = None

        if variable_name in rules.get("advisory_bands", {}):
            advisory = rules["advisory_bands"][variable_name]
            advisory_min = advisory.get("min")
            advisory_max = advisory.get("max")
            advisory_source = advisory.get("source")

            advisory_flag_col = f"flag_{variable_name}_outside_advisory_band"
            advisory_flag = pd.Series(False, index=df.index)

            if "min" in advisory and advisory["min"] is not None:
                advisory_flag = advisory_flag | (numeric_series.notna() & (numeric_series < advisory["min"]))

            if "max" in advisory and advisory["max"] is not None:
                advisory_flag = advisory_flag | (numeric_series.notna() & (numeric_series > advisory["max"]))

            df[advisory_flag_col] = advisory_flag
            advisory_count = int(advisory_flag.sum())
            advisory_pct = round(float(advisory_flag.mean() * 100), 6)

        variable_rows.append({
            "variable_name": variable_name,
            "hard_min": bounds.get("min"),
            "hard_max": bounds.get("max"),
            "hard_source": bounds.get("source"),
            "advisory_min": advisory_min,
            "advisory_max": advisory_max,
            "advisory_source": advisory_source,
            "non_null_count": int(numeric_series.notna().sum()),
            "missing_count": int(numeric_series.isna().sum()),
            "missing_pct": round(float(numeric_series.isna().mean() * 100), 6),
            "out_of_range_count": int(out_of_range.sum()),
            "out_of_range_pct": round(float(out_of_range.mean() * 100), 6),
            "advisory_count": advisory_count,
            "advisory_pct": advisory_pct
        })

global_block = (
    df["flag_missing_station_id"]
    | df["flag_missing_timestamp_utc"]
    | df["flag_station_never_active"]
    | df["flag_data_absent"]
    | df["flag_before_first_present"]
    | df["flag_after_last_present"]
    | df["flag_warmup"]
)

target_rows = []

for target_name, required_columns in rules["target_columns"].items():
    existing_columns = [c for c in required_columns if c in df.columns]

    if len(existing_columns) == 0:
        df[f"flag_{target_name}_missing_any"] = True
        df[f"flag_{target_name}_out_of_range_any"] = False
        df[f"gate_{target_name}_ready"] = False
    else:
        missing_any = df[existing_columns].isna().any(axis=1)

        range_flag_cols = [
            f"flag_{col}_out_of_range"
            for col in existing_columns
            if f"flag_{col}_out_of_range" in df.columns
        ]

        if len(range_flag_cols) > 0:
            out_of_range_any = df[range_flag_cols].any(axis=1)
        else:
            out_of_range_any = pd.Series(False, index=df.index)

        df[f"flag_{target_name}_missing_any"] = missing_any
        df[f"flag_{target_name}_out_of_range_any"] = out_of_range_any
        df[f"gate_{target_name}_ready"] = ~(global_block | missing_any | out_of_range_any)

    target_rows.append({
        "target_name": target_name,
        "ready_rows": int(df[f"gate_{target_name}_ready"].sum()),
        "ready_pct": round(float(df[f"gate_{target_name}_ready"].mean() * 100), 6),
        "missing_rows": int(df[f"flag_{target_name}_missing_any"].sum()),
        "missing_pct": round(float(df[f"flag_{target_name}_missing_any"].mean() * 100), 6),
        "out_of_range_rows": int(df[f"flag_{target_name}_out_of_range_any"].sum()),
        "out_of_range_pct": round(float(df[f"flag_{target_name}_out_of_range_any"].mean() * 100), 6)
    })

summary = {
    "row_count": int(len(df)),
    "station_count": int(df["station_id"].nunique()),
    "time_coverage_start_utc": None if df["timestamp_utc"].isna().all() else df["timestamp_utc"].min().strftime("%Y-%m-%d %H:%M:%S"),
    "time_coverage_end_utc": None if df["timestamp_utc"].isna().all() else df["timestamp_utc"].max().strftime("%Y-%m-%d %H:%M:%S"),
    "flag_missing_station_id_rows": int(df["flag_missing_station_id"].sum()),
    "flag_missing_timestamp_utc_rows": int(df["flag_missing_timestamp_utc"].sum()),
    "flag_station_never_active_rows": int(df["flag_station_never_active"].sum()),
    "flag_data_absent_rows": int(df["flag_data_absent"].sum()),
    "flag_before_first_present_rows": int(df["flag_before_first_present"].sum()),
    "flag_after_last_present_rows": int(df["flag_after_last_present"].sum()),
    "flag_warmup_rows": int(df["flag_warmup"].sum()),
    "targets": {
        row["target_name"]: {
            "ready_rows": row["ready_rows"],
            "ready_pct": row["ready_pct"]
        }
        for row in target_rows
    }
}

station_summary = df.groupby("station_id", dropna=False).agg(
    rows=("station_id", "size"),
    first_timestamp_utc=("timestamp_utc", "min"),
    last_timestamp_utc=("timestamp_utc", "max"),
    first_present_timestamp=("first_present_timestamp", "first"),
    last_present_timestamp=("last_present_timestamp", "first"),
    active_rows=("active_rows", "first"),
    total_rows=("total_rows", "first"),
    warmup_days=("warmup_days", "first"),
    data_absent_rows=("flag_data_absent", "sum"),
    before_first_present_rows=("flag_before_first_present", "sum"),
    after_last_present_rows=("flag_after_last_present", "sum"),
    warmup_rows=("flag_warmup", "sum")
).reset_index()

for target_name in rules["target_columns"].keys():
    target_station = df.groupby("station_id").agg(
        ready_rows=(f"gate_{target_name}_ready", "sum")
    ).reset_index().rename(columns={"ready_rows": f"{target_name}_ready_rows"})
    station_summary = station_summary.merge(target_station, on="station_id", how="left")
    station_summary[f"{target_name}_ready_pct"] = (station_summary[f"{target_name}_ready_rows"] / station_summary["rows"] * 100).round(6)

target_summary = pd.DataFrame(target_rows)
variable_summary = pd.DataFrame(variable_rows)

Path(args.output_validated_parquet).parent.mkdir(parents=True, exist_ok=True)
Path(args.output_summary_json).parent.mkdir(parents=True, exist_ok=True)
Path(args.output_station_csv).parent.mkdir(parents=True, exist_ok=True)
Path(args.output_target_csv).parent.mkdir(parents=True, exist_ok=True)
Path(args.output_variable_csv).parent.mkdir(parents=True, exist_ok=True)

df.to_parquet(args.output_validated_parquet, engine="pyarrow", index=False)

with open(args.output_summary_json, "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2)

station_summary.to_csv(args.output_station_csv, index=False)
target_summary.to_csv(args.output_target_csv, index=False)
variable_summary.to_csv(args.output_variable_csv, index=False)

print(json.dumps(summary, indent=2))