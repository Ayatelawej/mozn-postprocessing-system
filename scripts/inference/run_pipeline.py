from __future__ import annotations

import os
import sys

import pandas as pd

from postprocessing.inference.assembly import assemble_inference_frame
from postprocessing.inference.forecast_api import fetch_base_and_daily
from postprocessing.inference.frame import build_inference_frame
from postprocessing.inference.gating import run_gated_inference
from postprocessing.inference.observations_api import DEFAULT_OBS_URL, fetch_observations, parse_observations
from postprocessing.inference.output import build_payload, write_payload
from postprocessing.inference.station_metadata import load_registry, print_reconciliation, resolve_stations

MODELS = {
    "temperature": "HGB", "relative_humidity": "HGB", "dew_point": "HGB",
    "wind_speed": "Ridge", "wind_gust": "Ridge", "pressure": "Ridge",
    "uv": "HGB", "wind_direction": "Ridge",
}
LEADS = tuple(range(1, 73))
OUT_DIR = os.environ.get("FORECAST_DIR", "outputs/forecasts")


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

    T = obs_df["valid_time_utc"].max().floor("h")
    print("issue_time T =", T)

    asm, t = assemble_inference_frame(base, obs_df, res, T, daily=daily)
    inf = build_inference_frame(asm, T).reset_index(drop=True)
    print(f"issue rows: {len(inf)} | running {len(MODELS)} targets x {len(LEADS)} leads ...")

    g = run_gated_inference(inf, res, obs_df, T, LEADS, MODELS)
    payload = build_payload(g, res, T)
    path = write_payload(payload, OUT_DIR)

    print("\nstatus counts:")
    for k, v in g["status"].value_counts().items():
        print(f"  {k:16}{v}")
    fb = g[g["status"] == "fallback"]
    if len(fb):
        print("fallback reasons:")
        for k, v in fb["reason"].value_counts().items():
            print(f"  {k:24}{v}")

    print(f"\ncells: {len(g)} ({payload['n_stations']} stations x {len(MODELS)} targets x {len(LEADS)} leads)")
    print("wrote:  ", path)
    print("latest: ", path.parent / "latest.json")


if __name__ == "__main__":
    main()
