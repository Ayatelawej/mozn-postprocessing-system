import pandas as pd
from pathlib import Path

files = sorted(Path("data/external/openmeteo").glob("*_hourly.parquet"))
print(f"parquets: {len(files)}")

total_rows = 0
cols_seen = None
null_counts: dict[str, int] = {}

for f in files:
    df = pd.read_parquet(f)
    total_rows += len(df)
    cols_seen = set(df.columns) if cols_seen is None else cols_seen & set(df.columns)
    for c in df.columns:
        null_counts[c] = null_counts.get(c, 0) + int(df[c].isna().sum())

print(f"total hourly rows: {total_rows}")
print(f"common columns across all stations: {len(cols_seen) if cols_seen else 0}")
print()
print("columns with any nulls:")
any_nulls = False
for col in sorted(null_counts.keys()):
    n = null_counts[col]
    if n > 0:
        any_nulls = True
        pct = 100 * n / total_rows
        print(f"  {col:35s} {n:>8} ({pct:.2f}%)")
if not any_nulls:
    print("  (none)")
