from pathlib import Path
import argparse
import pandas as pd
import yaml

parser = argparse.ArgumentParser()
parser.add_argument("--input_csv", required=True)
parser.add_argument("--mapping_yaml", required=True)
parser.add_argument("--output_parquet", required=True)
args = parser.parse_args()

with open(args.mapping_yaml, "r", encoding="utf-8") as f:
    mapping = yaml.safe_load(f)

df = pd.read_csv(args.input_csv)
df = df.rename(columns=mapping["rename_map"])

for col in ["timestamp_utc", "hour_utc", "timestamp_utc_dt"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

numeric_columns = [
    "raw_record_count",
    "station_latitude",
    "station_longitude",
    "qc_status_raw",
    "epoch",
    "solar_radiation_wm2",
    "uv_index",
    "wind_direction_deg",
    "relative_humidity_pct",
    "relative_humidity_high_pct",
    "relative_humidity_low_pct",
    "temperature_c",
    "temperature_high_c",
    "temperature_low_c",
    "wind_speed_kmh",
    "wind_speed_high_kmh",
    "wind_speed_low_kmh",
    "wind_gust_kmh",
    "wind_gust_high_kmh",
    "wind_gust_low_kmh",
    "dew_point_c",
    "dew_point_high_c",
    "dew_point_low_c",
    "wind_chill_c",
    "wind_chill_high_c",
    "wind_chill_low_c",
    "heat_index_c",
    "heat_index_high_c",
    "heat_index_low_c",
    "pressure_max_hpa",
    "pressure_min_hpa",
    "pressure_trend_hpa",
    "rain_rate_mm_per_h",
    "rain_total_mm",
    "elevation_m",
    "data_present"
]

for col in numeric_columns:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

ordered_columns = [c for c in mapping["ordered_columns"] if c in df.columns]
remaining_columns = [c for c in df.columns if c not in ordered_columns]
df = df[ordered_columns + remaining_columns]

output_path = Path(args.output_parquet)
output_path.parent.mkdir(parents=True, exist_ok=True)
df.to_parquet(output_path, engine="pyarrow", index=False)

print(f"rows={len(df)}")
print(f"columns={len(df.columns)}")
print(f"output_file={output_path}")