from __future__ import annotations

import pandas as pd

from postprocessing.features.circular import add_circular_wind_features
from postprocessing.features.lags import add_lag_features
from postprocessing.features.radiation import add_radiation_features
from postprocessing.features.rain import add_rain_features
from postprocessing.features.solar import add_solar_features, merge_daily_into_hourly
from postprocessing.features.time import add_time_features
from postprocessing.targets.derived import add_derived_features
from postprocessing.targets.pressure import add_pressure_residuals
from postprocessing.targets.residuals import add_core_residuals
from postprocessing.targets.wind_direction import add_wind_direction_residual


FEATURE_BUILDERS = (
    add_time_features,
    add_circular_wind_features,
    add_solar_features,
    add_radiation_features,
    add_rain_features,
    add_lag_features,
    add_core_residuals,
    add_pressure_residuals,
    add_wind_direction_residual,
    add_derived_features,
)


def build_hourly_grid(issue_time, history_hours=24, horizon_hours=72):
    t = pd.Timestamp(issue_time)
    t = t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")
    t = t.floor("h")
    grid = pd.date_range(
        t - pd.Timedelta(hours=history_hours),
        t + pd.Timedelta(hours=horizon_hours),
        freq="h",
        tz="UTC",
    )
    return grid, t


def assemble_inference_frame(base_hourly, obs, resolutions, issue_time, *, daily=None, history_hours=24, horizon_hours=72):
    grid, t = build_hourly_grid(issue_time, history_hours, horizon_hours)
    stations = list(resolutions["station_id"])
    skeleton = pd.DataFrame(
        index=pd.MultiIndex.from_product([stations, grid], names=["station_id", "valid_time_utc"])
    ).reset_index()

    base = base_hourly.copy()
    base["valid_time_utc"] = pd.to_datetime(base["valid_time_utc"], utc=True)
    obs = obs.copy()
    obs["valid_time_utc"] = pd.to_datetime(obs["valid_time_utc"], utc=True)
    obs_cols = [c for c in obs.columns if c not in ("station_id", "valid_time_utc")]

    frame = skeleton.merge(base, on=["station_id", "valid_time_utc"], how="left")
    frame = frame.merge(obs[["station_id", "valid_time_utc"] + obs_cols], on=["station_id", "valid_time_utc"], how="left")

    meta = resolutions[["station_id", "latitude", "longitude", "elevation_m"]].rename(
        columns={"latitude": "station_latitude", "longitude": "station_longitude"}
    )
    frame = frame.merge(meta, on="station_id", how="left")
    frame = frame.sort_values(["station_id", "valid_time_utc"]).reset_index(drop=True)

    if daily is not None:
        frame = merge_daily_into_hourly(frame, daily)

    out = frame
    for fn in FEATURE_BUILDERS:
        out = fn(out)
    return out, t
