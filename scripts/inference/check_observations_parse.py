from __future__ import annotations

import sys

import pandas as pd

from postprocessing.inference.observations_api import CANONICAL_OBS_COLUMNS, build_station_frame
from postprocessing.inference.station_metadata import load_registry


def make_obs(t0, n=72, null_hours=()):
    rows = []
    for h in range(n):
        ts = (t0 - pd.Timedelta(hours=n - 1 - h)).strftime("%Y-%m-%dT%H:%M:%SZ")
        if h in null_hours:
            rows.append({
                "timestamp": ts,
                "temperature_c": None,
                "relative_humidity_pct": None,
                "dew_point_c": None,
                "wind_speed_kmh": None,
                "wind_gust_kmh": None,
                "wind_direction_deg": None,
                "pressure_max_hpa": None,
                "pressure_min_hpa": None,
                "rain_total_mm": None,
                "rain_rate_max_mmph": None,
                "uv_index": None,
                "solar_radiation_wm2": None,
                "sample_count": 0,
            })
        else:
            rows.append({
                "timestamp": ts,
                "temperature_c": 21.0 + (h % 5),
                "relative_humidity_pct": 55.0,
                "dew_point_c": 11.0,
                "wind_speed_kmh": 14.0,
                "wind_gust_kmh": 25.0,
                "wind_direction_deg": float((h * 13) % 360),
                "pressure_max_hpa": 1013.0,
                "pressure_min_hpa": 1011.0,
                "rain_total_mm": 0.0,
                "rain_rate_max_mmph": 0.0,
                "uv_index": 3.0,
                "solar_radiation_wm2": 400.0,
                "sample_count": 12,
            })
    return rows


def main():
    reg = load_registry()
    t0 = pd.Timestamp("2026-06-04T12:00:00Z")

    payload = {
        "message": "ok",
        "data": [
            {
                "station_id": "uuid-A",
                "name": "Roaya-Maya",
                "latitude": 32.78,
                "longitude": 12.86,
                "elevation": 0,
                "observations": make_obs(t0, null_hours=(3, 4)),
            },
            {
                "station_id": "uuid-B",
                "name": "Desert-New",
                "latitude": 27.0,
                "longitude": 17.0,
                "elevation": 0,
                "observations": make_obs(t0),
            },
        ],
    }

    frame, res = build_station_frame(payload, reg, prefer_backend=False)

    ok = True
    print("frame rows:", len(frame), "| stations:", frame["station_id"].nunique())
    ok = ok and len(frame) == 144 and frame["station_id"].nunique() == 2

    need = CANONICAL_OBS_COLUMNS + ["valid_time_utc", "station_latitude", "station_longitude", "elevation_m"]
    missing = [c for c in need if c not in frame.columns]
    print("missing canonical cols:", missing if missing else "none")
    ok = ok and not missing

    is_utc = str(frame["valid_time_utc"].dt.tz) == "UTC"
    print("valid_time_utc tz == UTC:", is_utc)
    ok = ok and is_utc

    for sid in ("uuid-A", "uuid-B"):
        s = frame[frame["station_id"] == sid].sort_values("valid_time_utc")
        diffs = s["valid_time_utc"].diff().dropna().dt.total_seconds() / 3600.0
        contig = bool((diffs == 1.0).all()) and len(s) == 72
        print(f"  {sid}: 72 contiguous hourly rows: {contig}")
        ok = ok and contig

    a = frame[frame["station_id"] == "uuid-A"].iloc[0]
    a_ok = abs(a["elevation_m"] - 8) < 1e-9 and abs(a["station_latitude"] - 32.784235) < 1e-3
    print(f"station A -> registry elevation 8 + precise lat: {a_ok} (elev={a['elevation_m']}, lat={a['station_latitude']})")
    ok = ok and a_ok

    b = frame[frame["station_id"] == "uuid-B"].iloc[0]
    b_ok = pd.isna(b["elevation_m"]) and abs(b["station_latitude"] - 27.0) < 1e-9
    print(f"station B (new) -> elevation pending + backend lat 27.0: {b_ok}")
    ok = ok and b_ok

    nullrows = frame[(frame["station_id"] == "uuid-A") & (frame["sample_count"] == 0)]
    null_ok = len(nullrows) == 2 and bool(nullrows["temperature_c"].isna().all())
    print(f"empty hours preserved with NaN metrics: {null_ok} ({len(nullrows)} rows)")
    ok = ok and null_ok

    print(f"\nOBS PARSE CHECK {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
