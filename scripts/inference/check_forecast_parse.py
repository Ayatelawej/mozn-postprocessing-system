from __future__ import annotations

import sys
import warnings

import numpy as np
import pandas as pd

from postprocessing.inference.assembly import assemble_inference_frame
from postprocessing.inference.forecast_api import HOURLY_VARIABLES, OPEN_METEO_TO_BASE, parse_forecast
from postprocessing.inference.frame import build_inference_frame
from postprocessing.inference.reconstruct import correct
from postprocessing.inference.station_metadata import load_registry, resolve_stations
from postprocessing.training.artifact_training import load_artifact
from postprocessing.training.feature_selection import features_for


OBS_COLS = [
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
]
OBS_FILL = {
    "temperature_c": 21.0,
    "relative_humidity_pct": 55.0,
    "dew_point_c": 11.0,
    "wind_speed_kmh": 14.0,
    "wind_gust_kmh": 25.0,
    "wind_direction_deg": 120.0,
    "pressure_max_hpa": 1013.0,
    "pressure_min_hpa": 1011.0,
    "rain_total_mm": 0.0,
    "uv_index": 3.0,
}


def hourly_vals(var, idx):
    day = ((idx.hour >= 5) & (idx.hour < 19)).astype(float)
    table = {
        "temperature_2m": 18 + 8 * day,
        "relative_humidity_2m": 60 - 20 * day,
        "dew_point_2m": 9.0,
        "apparent_temperature": 18 + 8 * day,
        "pressure_msl": 1013.0,
        "surface_pressure": 1006.0,
        "cloud_cover": 30.0,
        "cloud_cover_low": 10.0,
        "cloud_cover_mid": 10.0,
        "cloud_cover_high": 10.0,
        "visibility": 24000.0,
        "wind_speed_10m": 12.0,
        "wind_direction_10m": (idx.hour * 15 % 360).astype(float),
        "wind_gusts_10m": 22.0,
        "shortwave_radiation": 600 * day,
        "direct_radiation": 400 * day,
        "diffuse_radiation": 200 * day,
        "sunshine_duration": 3000 * day,
        "is_day": day.astype(int),
        "precipitation": 0.0,
        "rain": 0.0,
    }
    v = table[var]
    return (v if np.ndim(v) else np.full(len(idx), v)).tolist()


def main():
    warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

    T = pd.Timestamp("2026-06-04T12:00:00Z")
    hours = pd.date_range(T - pd.Timedelta(hours=30), T + pd.Timedelta(hours=80), freq="h", tz="UTC")
    days = pd.date_range(hours.min().floor("D"), hours.max().floor("D"), freq="D")
    payload = {
        "latitude": 32.78,
        "longitude": 12.86,
        "elevation": 7.0,
        "hourly": {
            "time": [t.strftime("%Y-%m-%dT%H:%M") for t in hours],
            **{v: hourly_vals(v, hours) for v in HOURLY_VARIABLES},
        },
        "daily": {
            "time": [d.strftime("%Y-%m-%d") for d in days],
            "sunrise": [d.strftime("%Y-%m-%dT05:00") for d in days],
            "sunset": [d.strftime("%Y-%m-%dT19:00") for d in days],
        },
    }

    base, daily, elev = parse_forecast(payload, "uuid-A")
    got_base = set(c for c in base.columns if c.startswith("base_"))
    missing_base = set(OPEN_METEO_TO_BASE.values()) - got_base
    print("base_* parsed:", len(got_base), "| daily cols:", [c for c in daily.columns if c.startswith("base_")], "| elevation:", elev)
    print("missing expected base cols:", missing_base if missing_base else "none")

    obs = pd.DataFrame({"station_id": "uuid-A", "valid_time_utc": hours})
    for c in OBS_COLS:
        obs[c] = OBS_FILL[c]

    reg = load_registry()
    res = resolve_stations([{"station_id": "uuid-A", "latitude": 32.78, "longitude": 12.86, "elevation": 0}], reg)

    asm, t = assemble_inference_frame(base, obs, res, T, daily=daily)
    solar_ok = all(c in asm.columns for c in ["minutes_from_sunrise", "solar_progress_0_1", "solar_centered"])
    print("assembled rows:", len(asm), "| solar cols present:", solar_ok)

    ok = not missing_base and solar_ok
    inf = build_inference_frame(asm, T).reset_index(drop=True)
    print(f"\nbuild_inference_frame issue rows: {len(inf)}")
    for tgt, mc in [
        ("temperature", "HGB"),
        ("uv", "HGB"),
        ("wind_direction", "Ridge"),
        ("pressure", "Ridge"),
        ("wind_speed", "Ridge"),
    ]:
        for lead in (1, 24, 72):
            feats = features_for(tgt, lead)
            missing = [c for c in feats if c not in inf.columns]
            art = load_artifact(f"models/artifacts/{tgt}_{mc}_lead{lead}h.joblib")
            corrected, keep = correct(art, inf, clamp=True)
            val = corrected[keep][0] if keep.sum() > 0 else float("nan")
            cov = "OK" if not missing else f"MISSING {missing[:3]}"
            print(f"  {tgt:14} L{lead:>2}: feat_cov={cov}  corrected={val:.3f}  finite={np.isfinite(val)}")
            ok = ok and not missing and keep.sum() > 0 and np.isfinite(val)

    print(f"\nFORECAST PARSE CHECK {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
