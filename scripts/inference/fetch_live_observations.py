from __future__ import annotations

import os
import sys

from postprocessing.inference.observations_api import (
    DEFAULT_OBS_URL,
    build_station_frame,
    fetch_observations,
)
from postprocessing.inference.station_metadata import load_registry, print_reconciliation


CANONICAL_NEED = [
    "temperature_c",
    "relative_humidity_pct",
    "dew_point_c",
    "wind_speed_kmh",
    "wind_gust_kmh",
    "wind_direction_deg",
    "pressure_max_hpa",
    "pressure_min_hpa",
    "rain_total_mm",
    "uv_index",
    "station_latitude",
    "station_longitude",
    "elevation_m",
]


def main():
    token = os.environ.get("AI_API_KEY", "").strip()
    base = os.environ.get("AI_OBS_URL", DEFAULT_OBS_URL).strip()
    if not token:
        print("NO TOKEN: set AI_API_KEY first")
        sys.exit(1)

    print("GET", base)
    payload = fetch_observations(token, base)
    reg = load_registry()
    frame, res = build_station_frame(payload, reg, prefer_backend=False)

    print()
    print_reconciliation(res)

    print()
    span = frame.groupby("station_id")["valid_time_utc"].agg(["min", "max", "count"])
    print(span.to_string())

    if "sample_count" in frame.columns:
        empty = frame.groupby("station_id")["sample_count"].apply(lambda s: int((s.fillna(0) == 0).sum()))
        print("\nempty hours (sample_count == 0) per station:")
        print(empty.to_string())

    missing = [c for c in CANONICAL_NEED if c not in frame.columns]
    print("\nmissing canonical cols:", missing if missing else "none")
    print("total rows:", len(frame), "| stations:", frame["station_id"].nunique())


if __name__ == "__main__":
    main()
