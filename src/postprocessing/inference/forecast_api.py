from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request

import pandas as pd


FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "pressure_msl",
    "surface_pressure",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "visibility",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",
    "sunshine_duration",
    "is_day",
    "precipitation",
    "rain",
]

DAILY_VARIABLES = ["sunrise", "sunset"]

OPEN_METEO_TO_BASE = {
    "temperature_2m": "base_temperature_c",
    "relative_humidity_2m": "base_relative_humidity_pct",
    "dew_point_2m": "base_dew_point_c",
    "wind_speed_10m": "base_wind_speed_kmh",
    "wind_gusts_10m": "base_wind_gust_kmh",
    "wind_direction_10m": "base_wind_direction_deg",
    "surface_pressure": "base_surface_pressure_hpa",
    "pressure_msl": "base_msl_pressure_hpa",
    "rain": "base_rain_total_mm",
    "shortwave_radiation": "base_solar_radiation_wm2",
    "cloud_cover": "base_cloud_cover_pct",
    "cloud_cover_low": "base_cloud_cover_low_pct",
    "cloud_cover_mid": "base_cloud_cover_mid_pct",
    "cloud_cover_high": "base_cloud_cover_high_pct",
    "precipitation": "base_precipitation_mm",
    "is_day": "base_is_day",
    "sunshine_duration": "base_sunshine_seconds",
    "direct_radiation": "base_direct_radiation_wm2",
    "diffuse_radiation": "base_diffuse_radiation_wm2",
    "apparent_temperature": "base_apparent_temperature_c",
    "visibility": "base_visibility_m",
}

DAILY_TO_BASE = {"sunrise": "base_sunrise_utc", "sunset": "base_sunset_utc"}


def build_forecast_url(lat, lon, past_days=3, forecast_days=4):
    params = {
        "latitude": f"{lat:.4f}",
        "longitude": f"{lon:.4f}",
        "past_days": str(past_days),
        "forecast_days": str(forecast_days),
        "hourly": ",".join(HOURLY_VARIABLES),
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": "UTC",
    }
    return f"{FORECAST_URL}?{urllib.parse.urlencode(params)}"


def fetch_forecast(lat, lon, past_days=3, forecast_days=4, timeout=60):
    url = build_forecast_url(lat, lon, past_days, forecast_days)
    req = urllib.request.Request(url, headers={"User-Agent": "mozn-inference/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def parse_forecast(payload, station_id):
    hourly = payload.get("hourly", {})
    base = pd.DataFrame({"valid_time_utc": pd.to_datetime(hourly.get("time", []), utc=True)})
    for var in HOURLY_VARIABLES:
        if var in hourly:
            base[var] = hourly[var]
    base = base.rename(columns=OPEN_METEO_TO_BASE)
    base["valid_time_utc"] = base["valid_time_utc"].dt.floor("h")
    base["station_id"] = station_id

    daily_in = payload.get("daily", {})
    daily = pd.DataFrame({"date_utc": pd.to_datetime(daily_in.get("time", []), utc=True)})
    for var in DAILY_VARIABLES:
        if var in daily_in:
            daily[var] = daily_in[var]
    daily = daily.rename(columns=DAILY_TO_BASE)
    for col in ("base_sunrise_utc", "base_sunset_utc"):
        if col in daily.columns:
            daily[col] = pd.to_datetime(daily[col], utc=True)
    daily["station_id"] = station_id

    return base, daily, payload.get("elevation")


def fetch_base_and_daily(resolutions, *, past_days=3, forecast_days=4, sleep_sec=0.4):
    base_frames = []
    daily_frames = []
    elevations = {}
    for _, r in resolutions.iterrows():
        sid = r["station_id"]
        payload = fetch_forecast(float(r["latitude"]), float(r["longitude"]), past_days, forecast_days)
        base, daily, elev = parse_forecast(payload, sid)
        base_frames.append(base)
        daily_frames.append(daily)
        elevations[sid] = elev
        time.sleep(sleep_sec)
    return pd.concat(base_frames, ignore_index=True), pd.concat(daily_frames, ignore_index=True), elevations
