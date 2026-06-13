from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

from postprocessing.inference.assembly import assemble_inference_frame
from postprocessing.inference.forecast_api import fetch_base_and_daily
from postprocessing.inference.frame import build_inference_frame
from postprocessing.inference.gating import run_gated_inference
from postprocessing.inference.observations_api import DEFAULT_OBS_URL, fetch_observations, parse_observations
from postprocessing.inference.station_metadata import load_registry, print_reconciliation, resolve_stations

MODELS = {
    "temperature": "HGB", "relative_humidity": "HGB", "dew_point": "HGB",
    "wind_speed": "Ridge", "wind_gust": "Ridge", "pressure": "Ridge",
    "uv": "HGB", "wind_direction": "Ridge",
}
LEADS = (1, 24, 72)
BOARD_LEAD = 24
GLYPH = {"ok": "", "low_confidence": "~", "fallback": "!"}


def status_board(g, resolutions, lead):
    sub = g[g["lead"] == lead]
    targets = list(MODELS.keys())
    print(f"\nstatus board @ lead {lead}h   (~ low_confidence, ! fallback to raw):")
    print("  " + f"{'station':>11}{'obs%':>6}  " + "".join(f"{t[:9]:>11}" for t in targets))
    for sid in resolutions["station_id"]:
        rs = {r["target"]: r for _, r in sub[sub["station_id"] == sid].iterrows()}
        mv = resolutions.loc[resolutions["station_id"] == sid, "matched_wu_id"].iloc[0]
        wu = ("NEW" if pd.isna(mv) else str(mv))[:11]
        of = (rs[targets[0]]["recent_obs_frac"] if targets[0] in rs else 0.0)
        cells = []
        for t in targets:
            r = rs.get(t)
            if r is None:
                cells.append(f"{'-':>11}")
            else:
                v = r["value"]
                txt = f"{v:.1f}{GLYPH[r['status']]}" if pd.notna(v) else f"nan{GLYPH[r['status']]}"
                cells.append(f"{txt:>11}")
        print(f"  {wu:>11}{of:>6.0%}  " + "".join(cells))


def summary(g):
    print("\nstatus counts (all leads x stations x targets):")
    for k, v in g["status"].value_counts().items():
        print(f"  {k:16} {v}")
    fb = g[g["status"] == "fallback"]
    if len(fb):
        print("fallback reasons:")
        for k, v in fb["reason"].value_counts().items():
            print(f"  {k:24} {v}")


def main():
    token = os.environ.get("AI_API_KEY", "").strip()
    if not token:
        print("NO TOKEN: run  $env:AI_API_KEY = \"...\"  first")
        sys.exit(1)
    obs_url = os.environ.get("AI_OBS_URL", DEFAULT_OBS_URL).strip()

    print("Fetching observations:", obs_url)
    obs_df, meta = parse_observations(fetch_observations(token, obs_url))
    reg = load_registry()
    res = resolve_stations(meta.to_dict("records"), reg)

    print("Fetching Open-Meteo forecast for", len(res), "stations")
    base, daily, elevations = fetch_base_and_daily(res)
    for i, r in res.iterrows():
        if bool(r["needs_openmeteo_elevation"]) and elevations.get(r["station_id"]) is not None:
            res.at[i, "elevation_m"] = float(elevations[r["station_id"]])

    print()
    print_reconciliation(res)
    matched = int((~res["needs_openmeteo_elevation"].astype(bool)).sum())
    print(f"\nbackend check: {len(res)} stations, {matched} matched to registry, {len(res) - matched} new/unmatched")

    T = obs_df["valid_time_utc"].max().floor("h")
    print("issue_time T =", T)

    asm, t = assemble_inference_frame(base, obs_df, res, T, daily=daily)
    inf = build_inference_frame(asm, T).reset_index(drop=True)
    print("issue rows built:", len(inf), "of", len(res), "stations")

    g = run_gated_inference(inf, res, obs_df, T, LEADS, MODELS)
    status_board(g, res, BOARD_LEAD)
    summary(g)


if __name__ == "__main__":
    main()
