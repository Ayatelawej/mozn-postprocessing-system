from pathlib import Path
import argparse
import shutil
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--input_csv", required=True)
parser.add_argument("--output_dir", required=True)
parser.add_argument("--overwrite", action="store_true")
args = parser.parse_args()

output_dir = Path(args.output_dir)

if output_dir.exists() and args.overwrite:
    shutil.rmtree(output_dir)

output_dir.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(args.input_csv)
df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
df["year"] = df["timestamp_utc"].dt.year.astype("Int64")
df["month"] = df["timestamp_utc"].dt.month.astype("Int64")

df.to_parquet(
    output_dir,
    engine="pyarrow",
    index=False,
    partition_cols=["station_id", "year", "month"]
)

print(f"rows={len(df)}")
print(f"stations={df['station_id'].nunique()}")
print(f"output_dir={output_dir}")