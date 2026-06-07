from __future__ import annotations

import os
import sys
import warnings

import numpy as np
import pandas as pd

from postprocessing.inference.assembly import assemble_inference_frame
from postprocessing.inference.forecast_api import fetch_base_and_daily
from postprocessing.inference.frame import build_inference_frame
from postprocessing.inference.observations_api import DEFAULT_OBS_URL, fetch_observations, parse_observations
from postprocessing.inference.reconstruct import correct
from postprocessing.inference.station_metadata import load_registry, print_reconciliation, resolve_stations
from postprocessing.training.artifact_training import load_artifact


MODELS = {
    "temperature": "HGB",
    "relative_humidity": "HGB",
    "dew_point": "HGB",
    "wind_speed": "Ridge",
    "wind_gust": "Ridge",
    "pressure": "Ridge",
    "uv": "HGB",
    "wind_direction": "Ridge",
}
LEAD = 24


def main():
    warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

    token = os.environ.get("AI_API_KEY", "").strip()
    if not token:
        print("NO TOKEN: set AI_API_KEY first")
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
    print("\nissue_time T =", T)

    empty = obs_df.groupby("station_id")["sample_count"].apply(lambda s: int((s.fillna(0) == 0).sum()))
    name_of = dict(zip(res["station_id"], res["matched_wu_id"], strict=False))

    asm, t = assemble_inference_frame(base, obs_df, res, T, daily=daily)
    inf = build_inference_frame(asm, T).reset_index(drop=True)
    sids = inf["station_id"].tolist()
    print("issue rows built:", len(inf), "of", len(res), "stations")

    results = {}
    n_finite = 0
    for tgt, mc in MODELS.items():
        art = load_artifact(f"models/artifacts/{tgt}_{mc}_lead{LEAD}h.joblib")
        corrected, keep = correct(art, inf, clamp=True)
        results[tgt] = {sids[i]: (corrected[i] if keep[i] else np.nan) for i in range(len(sids))}
        n_finite += int(np.isfinite(corrected[keep]).sum())

    print(f"\ncorrected values at lead {LEAD}h per station:")
    print("  " + f"{'station':>10} {'empty_h':>8}  " + "  ".join(f"{t[:7]:>8}" for t in MODELS))
    for sid in sids:
        wu = str(name_of.get(sid, "-"))[:10]
        vals = "  ".join(f"{results[t][sid]:>8.2f}" if np.isfinite(results[t][sid]) else f"{'nan':>8}" for t in MODELS)
        print(f"  {wu:>10} {empty.get(sid, 0):>8}  {vals}")

    print(f"\ntotal finite predictions: {n_finite} over {len(sids)} stations x {len(MODELS)} targets at L{LEAD}")
    print("note: fully-empty stations still produce values here via imputation; the Part 4 gate routes them to fallback")


if __name__ == "__main__":
    main()
