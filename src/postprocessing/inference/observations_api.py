from __future__ import annotations

import json
import ssl
import urllib.request

import pandas as pd

from postprocessing.inference.station_metadata import resolve_stations


DEFAULT_OBS_URL = "https://mozn.org.ly/api/ai/observations"

CANONICAL_OBS_COLUMNS = [
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
    "sample_count",
]


def fetch_observations(token, base_url=DEFAULT_OBS_URL, station_id=None, timeout=60):
    url = base_url + (f"?station_id={station_id}" if station_id else "")
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout, context=ssl.create_default_context()) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def _parse_ts(series):
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_datetime(series, unit="s", utc=True)
    return pd.to_datetime(series, utc=True)


def parse_observations(payload):
    data = payload.get("data") if isinstance(payload, dict) else payload
    if isinstance(data, str):
        data = json.loads(data)
    if not isinstance(data, list):
        raise ValueError("observations payload 'data' is not a list of stations")

    frames = []
    meta = []
    for st in data:
        uuid = st.get("station_id")
        obs = st.get("observations") or []
        if obs:
            df = pd.DataFrame(obs)
            df["station_id"] = uuid
            frames.append(df)
        meta.append({
            "station_id": uuid,
            "name": st.get("name"),
            "latitude": st.get("latitude"),
            "longitude": st.get("longitude"),
            "elevation": st.get("elevation"),
        })

    obs_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not obs_df.empty:
        obs_df["valid_time_utc"] = _parse_ts(obs_df["timestamp"])
        for col in obs_df.columns:
            if col not in ("timestamp", "station_id", "valid_time_utc"):
                obs_df[col] = pd.to_numeric(obs_df[col], errors="coerce")
        obs_df = obs_df.sort_values(["station_id", "valid_time_utc"]).reset_index(drop=True)
    return obs_df, pd.DataFrame(meta)


def build_station_frame(payload, registry, *, prefer_backend=False):
    obs_df, meta = parse_observations(payload)
    resolutions = resolve_stations(meta.to_dict("records"), registry, prefer_backend=prefer_backend)
    metacols = resolutions[["station_id", "latitude", "longitude", "elevation_m"]].rename(
        columns={"latitude": "station_latitude", "longitude": "station_longitude"}
    )
    frame = obs_df.merge(metacols, on="station_id", how="left")
    return frame, resolutions
