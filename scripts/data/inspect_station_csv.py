from pathlib import Path
import argparse
import json
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--input_csv", required=True)
parser.add_argument("--output_json", required=True)
parser.add_argument("--output_columns_csv", required=True)
args = parser.parse_args()

input_path = Path(args.input_csv)
df = pd.read_csv(input_path)

timestamp_column = "timestamp_utc" if "timestamp_utc" in df.columns else "hour_utc"
timestamp_series = pd.to_datetime(df[timestamp_column], utc=True, errors="coerce")

profile = {
    "file_name": input_path.name,
    "row_count": int(len(df)),
    "column_count": int(len(df.columns)),
    "station_count": int(df["station_id"].nunique()) if "station_id" in df.columns else None,
    "timestamp_column": timestamp_column,
    "time_coverage_start_utc": None if timestamp_series.isna().all() else timestamp_series.min().strftime("%Y-%m-%d %H:%M:%S"),
    "time_coverage_end_utc": None if timestamp_series.isna().all() else timestamp_series.max().strftime("%Y-%m-%d %H:%M:%S"),
    "duplicate_station_timestamp_rows": int(df.duplicated(subset=["station_id", timestamp_column]).sum()) if "station_id" in df.columns and timestamp_column in df.columns else None,
    "columns": list(df.columns)
}

column_profile = pd.DataFrame({
    "column_name": df.columns,
    "dtype": [str(df[c].dtype) for c in df.columns],
    "non_null_count": [int(df[c].notna().sum()) for c in df.columns],
    "missing_count": [int(df[c].isna().sum()) for c in df.columns],
    "missing_pct": [round(float(df[c].isna().mean() * 100), 6) for c in df.columns],
    "example_value": [None if df[c].dropna().empty else str(df[c].dropna().iloc[0]) for c in df.columns]
})

Path(args.output_json).parent.mkdir(parents=True, exist_ok=True)
Path(args.output_columns_csv).parent.mkdir(parents=True, exist_ok=True)

with open(args.output_json, "w", encoding="utf-8") as f:
    json.dump(profile, f, indent=2)

column_profile.to_csv(args.output_columns_csv, index=False)

print(json.dumps(profile, indent=2))