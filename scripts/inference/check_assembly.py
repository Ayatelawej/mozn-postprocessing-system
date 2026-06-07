from __future__ import annotations

import sys
import warnings

import numpy as np
import pandas as pd

from postprocessing.inference.assembly import assemble_inference_frame
from postprocessing.inference.frame import build_inference_frame
from postprocessing.inference.reconstruct import correct
from postprocessing.training.artifact_training import load_artifact
from postprocessing.training.feature_selection import features_for


APRIL_PATH = r"data/processed/april2026_eval_frame_v3.parquet"
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
COMPARE = [
    "hour_sin",
    "hour_cos",
    "station_wind_u_kmh",
    "base_wind_u_kmh",
    "temperature_residual_c",
    "winddir_residual_sin",
    "winddir_residual_cos",
    "pressure_residual_avg_hpa",
    "minutes_from_sunrise",
    "solar_progress_0_1",
    "clipped_shortwave_wm2",
    "uv_proxy",
    "temperature_c_lag_1h",
    "temperature_c_lag_24h",
    "temperature_c_roll_mean_24h",
    "heat_index_c",
    "wind_chill_c",
]


def main():
    warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

    april = pd.read_parquet(APRIL_PATH)
    april["valid_time_utc"] = pd.to_datetime(april["valid_time_utc"], utc=True)
    base_cols = [c for c in april.columns if c.startswith("base_")]
    obs_cols = [c for c in OBS_COLS if c in april.columns]

    counts = april.groupby("station_id")["valid_time_utc"].count().sort_values(ascending=False)
    stations = list(counts.index[:2])
    sdf = april[april["station_id"] == stations[0]].sort_values("valid_time_utc")
    T = sdf["valid_time_utc"].iloc[len(sdf) // 2].floor("h")
    print("stations:", stations, "| issue T:", T)

    base_hourly = april[["station_id", "valid_time_utc"] + base_cols].copy()
    obs = april[["station_id", "valid_time_utc"] + obs_cols].copy()
    res = april.groupby("station_id").agg(
        latitude=("station_latitude", "first"),
        longitude=("station_longitude", "first"),
        elevation_m=("elevation_m", "first"),
    ).reset_index()
    res = res[res["station_id"].isin(stations)].reset_index(drop=True)

    asm, t = assemble_inference_frame(base_hourly, obs, res, T)
    print("assembled rows:", len(asm), "| per station:", len(asm) // len(stations))

    ok = True
    cmp_cols = [c for c in COMPARE if c in asm.columns and c in april.columns]
    print(f"comparing {len(cmp_cols)} derived cols at issue row T across {len(stations)} stations")
    maxdiff = 0.0
    for s in stations:
        a_row = asm[(asm["station_id"] == s) & (asm["valid_time_utc"] == T)]
        o_row = april[(april["station_id"] == s) & (april["valid_time_utc"] == T)]
        if len(a_row) != 1 or len(o_row) != 1:
            print(f"  {s}: row lookup failed")
            ok = False
            continue
        a = a_row.iloc[0]
        o = o_row.iloc[0]
        bad = []
        for c in cmp_cols:
            av, ov = a[c], o[c]
            if pd.isna(av) and pd.isna(ov):
                continue
            if pd.isna(av) != pd.isna(ov) or abs(float(av) - float(ov)) > 1e-9:
                bad.append(c)
            else:
                maxdiff = max(maxdiff, abs(float(av) - float(ov)))
        print(f"  {s}: {'OK' if not bad else 'MISMATCH ' + str(bad[:4])}")
        ok = ok and not bad
    print(f"max abs diff on matched derived cols: {maxdiff:.2e}")

    inf = build_inference_frame(asm, T)
    inf_s = inf[inf["station_id"] == stations[0]].reset_index(drop=True)
    print(f"\nbuild_inference_frame on assembled: {len(inf_s)} issue row(s) for station 0")
    for tgt, mc in [
        ("temperature", "HGB"),
        ("uv", "HGB"),
        ("wind_direction", "Ridge"),
        ("pressure", "Ridge"),
    ]:
        for lead in (1, 24, 72):
            feats = features_for(tgt, lead)
            missing = [c for c in feats if c not in inf_s.columns]
            art = load_artifact(f"models/artifacts/{tgt}_{mc}_lead{lead}h.joblib")
            corrected, keep = correct(art, inf_s, clamp=True)
            val = corrected[keep][0] if keep.sum() > 0 else float("nan")
            cov = "OK" if not missing else f"MISSING {missing[:3]}"
            print(f"  {tgt:14} L{lead:>2}: feat_cov={cov}  corrected={val:.3f}  predicted_for={int(keep.sum())} row")
            ok = ok and not missing and keep.sum() > 0 and np.isfinite(val)

    print(f"\nASSEMBLY CHECK {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
