from __future__ import annotations

import glob
import json
import sys
import tempfile
from pathlib import Path

import pandas as pd

from postprocessing.inference.frame import build_inference_frame
from postprocessing.inference.gating import run_gated_inference
from postprocessing.inference.output import build_payload, write_payload

MODELS = {"temperature": "HGB", "relative_humidity": "HGB", "dew_point": "HGB", "wind_speed": "Ridge",
          "wind_gust": "Ridge", "pressure": "Ridge", "uv": "HGB", "wind_direction": "Ridge"}
LEADS = (1, 24, 72)
APRIL_PATH = r"data/processed/april2026_eval_frame_v3.parquet"


def main():
    ok = True
    have = {Path(p).name for p in glob.glob("models/artifacts/*_lead*h.joblib")}
    missing = [f"{t}_{m}_lead{h}h.joblib" for t, m in MODELS.items() for h in range(1, 73)
               if f"{t}_{m}_lead{h}h.joblib" not in have]
    print(f"artifact inventory: {8 * 72 - len(missing)}/{8 * 72} present", "OK" if not missing else f"MISSING {missing[:3]}")
    ok = ok and not missing

    april = pd.read_parquet(APRIL_PATH)
    april["valid_time_utc"] = pd.to_datetime(april["valid_time_utc"], utc=True)
    st = list(april.groupby("station_id")["valid_time_utc"].count().sort_values(ascending=False).index[:2])
    sdf = april[april["station_id"] == st[0]].sort_values("valid_time_utc")
    T = sdf["valid_time_utc"].iloc[len(sdf) // 2].floor("h")
    inf = build_inference_frame(april, T)
    inf = inf[inf["station_id"].isin(st)].drop_duplicates("station_id").reset_index(drop=True)
    obs_rows = [{"station_id": s, "valid_time_utc": T - pd.Timedelta(hours=h), "sample_count": 12, "temperature_c": 20.0}
                for s in st for h in range(24)]
    res = pd.DataFrame({"station_id": st, "matched_wu_id": ["ITRIPO33", "IJANZO3"], "needs_openmeteo_elevation": [False, False]})

    g = run_gated_inference(inf, res, pd.DataFrame(obs_rows), T, LEADS, MODELS)
    payload = build_payload(g, res, T)

    checks = {
        "n_stations==2": payload["n_stations"] == 2,
        "each station has 8 targets": all(len(s["targets"]) == 8 for s in payload["stations"]),
        "each target has len(LEADS) entries": all(len(v) == len(LEADS) for s in payload["stations"] for v in s["targets"].values()),
        "entry keys correct": all(set(e) == {"lead", "valid_time_utc", "value", "baseline", "status", "reason"}
                                  for s in payload["stations"] for v in s["targets"].values() for e in v),
        "valid_time = issue + lead": all(e["valid_time_utc"] == (T + pd.Timedelta(hours=e["lead"])).strftime("%Y-%m-%dT%H:%M:%SZ")
                                         for s in payload["stations"] for v in s["targets"].values() for e in v),
        "values float or None": all((e["value"] is None or type(e["value"]) is float)
                                    for s in payload["stations"] for v in s["targets"].values() for e in v),
        "wu_id populated": all(s["wu_id"] in ("ITRIPO33", "IJANZO3") for s in payload["stations"]),
    }
    checks["json round-trip"] = json.loads(json.dumps(payload))["n_stations"] == 2

    for name, good in checks.items():
        print(f"  {'OK' if good else 'BAD'}  {name}"); ok = ok and good

    with tempfile.TemporaryDirectory() as tmp:
        path = write_payload(payload, tmp)
        back = json.loads(path.read_text())
        latest = json.loads((path.parent / "latest.json").read_text())
        wrote = back == payload and latest == payload and path.name.startswith("forecast_")
        print(f"  {'OK' if wrote else 'BAD'}  write_payload round-trips ({path.name})"); ok = ok and wrote

    print(f"\nOUTPUT CHECK {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
