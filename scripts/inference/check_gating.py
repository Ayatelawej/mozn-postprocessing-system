from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from postprocessing.inference.frame import build_inference_frame
from postprocessing.inference.gating import (DARK_FALLBACK, decide_cell, fallback_value,
                                             recent_obs_fraction, run_gated_inference)
from postprocessing.training.artifact_training import load_artifact

MODELS = {"temperature": "HGB", "relative_humidity": "HGB", "dew_point": "HGB", "wind_speed": "Ridge",
          "wind_gust": "Ridge", "pressure": "Ridge", "uv": "HGB", "wind_direction": "Ridge"}
LEADS = (24,)
APRIL_PATH = r"data/processed/april2026_eval_frame_v3.parquet"


def main():
    ok = True

    T = pd.Timestamp("2026-06-07T11:00:00Z")
    rows = []
    for sid, sc in [("A", 12), ("B", 0), ("C", 12)]:
        for h in range(24):
            n = sc if (sid != "C" or h < 12) else 0
            rows.append({"station_id": sid, "valid_time_utc": T - pd.Timedelta(hours=h),
                         "sample_count": n, "temperature_c": 20.0 if n > 0 else np.nan})
    frac = recent_obs_fraction(pd.DataFrame(rows), T)
    rec_ok = abs(frac["A"] - 1.0) < 1e-9 and abs(frac["B"]) < 1e-9 and abs(frac["C"] - 0.5) < 1e-9
    print(f"recency: A={frac['A']:.2f} B={frac['B']:.2f} C={frac['C']:.2f}  {'OK' if rec_ok else 'BAD'}")
    ok = ok and rec_ok

    april = pd.read_parquet(APRIL_PATH)
    april["valid_time_utc"] = pd.to_datetime(april["valid_time_utc"], utc=True)
    counts = april.groupby("station_id")["valid_time_utc"].count().sort_values(ascending=False)
    st = list(counts.index[:3])
    sdf = april[april["station_id"] == st[0]].sort_values("valid_time_utc")
    Ti = sdf["valid_time_utc"].iloc[len(sdf) // 2].floor("h")
    inf_all = build_inference_frame(april, Ti)
    inf = inf_all[inf_all["station_id"].isin(st)].drop_duplicates("station_id")
    inf = inf.sort_values("station_id", key=lambda s: s.map({v: i for i, v in enumerate(st)})).reset_index(drop=True)

    obs_rows = []
    for sid, sc in [(st[0], 12), (st[1], 0), (st[2], 0)]:
        for h in range(24):
            obs_rows.append({"station_id": sid, "valid_time_utc": Ti - pd.Timedelta(hours=h),
                             "sample_count": sc, "temperature_c": 20.0 if sc > 0 else np.nan})
    obs_df = pd.DataFrame(obs_rows)
    res = pd.DataFrame({"station_id": st, "matched_wu_id": ["KNOWN_A", "KNOWN_B", None],
                        "needs_openmeteo_elevation": [False, False, True]})

    g = run_gated_inference(inf, res, obs_df, Ti, LEADS, MODELS)

    def cell(sid, t):
        r = g[(g["station_id"] == sid) & (g["target"] == t)].iloc[0]
        return r["status"], r["reason"]

    print("\nstation0 known+sufficient -> ok:")
    for t in MODELS:
        s, r = cell(st[0], t); good = s == "ok"; ok = ok and good
        print(f"  {t:18}{s:14}{r:24}{'OK' if good else 'BAD'}")
    print("station1 known+dark -> low_conf keep / fallback pressure,wind_dir:")
    for t in MODELS:
        s, r = cell(st[1], t)
        exp = ("fallback", "insufficient_recent_obs") if t in DARK_FALLBACK else ("low_confidence", "insufficient_recent_obs")
        good = (s, r) == exp; ok = ok and good
        print(f"  {t:18}{s:14}{r:24}{'OK' if good else 'BAD ' + str(exp)}")
    print("station2 new+dark -> fallback/new_station_unvalidated:")
    for t in MODELS:
        s, r = cell(st[2], t); good = (s, r) == ("fallback", "new_station_unvalidated"); ok = ok and good
        print(f"  {t:18}{s:14}{r:24}{'OK' if good else 'BAD'}")

    art = load_artifact("models/artifacts/temperature_HGB_lead24h.joblib")
    row0 = inf_all[inf_all["station_id"] == st[0]].iloc[0]
    base = fallback_value("temperature", art, row0, 24)
    v, s, r = decide_cell("temperature", 24, art, row0, base + 50.0, True, True, False)
    qc = (s == "fallback" and r == "output_qc_failed" and abs(v - base) < 1e-9); ok = ok and qc
    print(f"\nQC trip (base+50): {s}/{r} value={v:.2f} base={base:.2f} {'OK' if qc else 'BAD'}")
    v, s, r = decide_cell("temperature", 24, art, row0, np.nan, False, True, False)
    stx = (s == "fallback" and r == "structural_missing"); ok = ok and stx
    print(f"structural (kept=False): {s}/{r} {'OK' if stx else 'BAD'}")

    print(f"\nGATING CHECK {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
