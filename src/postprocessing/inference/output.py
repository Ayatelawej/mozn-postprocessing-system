from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def _iso(ts):
    return pd.Timestamp(ts).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def build_payload(gated, resolutions, issue_time):
    t = pd.Timestamp(issue_time)
    t = t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")
    wu = dict(zip(resolutions["station_id"], resolutions["matched_wu_id"]))
    stations = []
    for sid, sdf in gated.groupby("station_id", sort=False):
        targets = {}
        for tgt, tdf in sdf.groupby("target", sort=False):
            entries = []
            for _, r in tdf.sort_values("lead").iterrows():
                v = r["value"]
                b = r["baseline"]
                entries.append({
                    "lead": int(r["lead"]),
                    "valid_time_utc": _iso(t + pd.Timedelta(hours=int(r["lead"]))),
                    "value": None if pd.isna(v) else round(float(v), 2),
                    "baseline": None if pd.isna(b) else round(float(b), 2),
                    "status": str(r["status"]),
                    "reason": str(r["reason"]),
                })
            targets[str(tgt)] = entries
        mv = wu.get(sid)
        stations.append({
            "station_id": str(sid),
            "wu_id": None if mv is None or (isinstance(mv, float) and pd.isna(mv)) else str(mv),
            "recent_obs_frac": float(sdf["recent_obs_frac"].iloc[0]),
            "targets": targets,
        })
    return {
        "issue_time_utc": _iso(t),
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "n_stations": len(stations),
        "stations": stations,
    }


def write_payload(payload, out_dir="outputs/forecasts"):
    d = Path(out_dir)
    d.mkdir(parents=True, exist_ok=True)
    stamp = payload["issue_time_utc"].replace(":", "").replace("-", "")
    text = json.dumps(payload, indent=2)
    path = d / f"forecast_{stamp}.json"
    path.write_text(text, encoding="utf-8")
    (d / "latest.json").write_text(text, encoding="utf-8")
    return path
