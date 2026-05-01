from __future__ import annotations

import numpy as np
import pandas as pd


def wrap_angle_deg(angle: pd.Series | np.ndarray | float) -> pd.Series | np.ndarray | float:
    return ((angle + 180.0) % 360.0) - 180.0


def circular_diff_deg(
    a: pd.Series | np.ndarray | float,
    b: pd.Series | np.ndarray | float,
) -> pd.Series | np.ndarray | float:
    return wrap_angle_deg(a - b)


def add_direction_sin_cos(
    df: pd.DataFrame,
    direction_col: str,
    prefix: str,
) -> pd.DataFrame:
    out = df.copy()
    if direction_col not in out.columns:
        return out
    radians = np.deg2rad(out[direction_col])
    out[f"{prefix}_sin"] = np.sin(radians)
    out[f"{prefix}_cos"] = np.cos(radians)
    return out


def add_wind_uv(
    df: pd.DataFrame,
    speed_col: str,
    direction_col: str,
    u_col: str,
    v_col: str,
) -> pd.DataFrame:
    out = df.copy()
    if speed_col not in out.columns or direction_col not in out.columns:
        return out
    radians = np.deg2rad(out[direction_col])
    speed = out[speed_col]
    out[u_col] = speed * np.sin(radians)
    out[v_col] = speed * np.cos(radians)
    return out


def add_circular_wind_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df
    out = add_direction_sin_cos(out, "wind_direction_deg", "station_wind_direction")
    out = add_direction_sin_cos(out, "base_wind_direction_deg", "base_wind_direction")
    out = add_wind_uv(out, "wind_speed_kmh", "wind_direction_deg", "station_wind_u_kmh", "station_wind_v_kmh")
    out = add_wind_uv(out, "base_wind_speed_kmh", "base_wind_direction_deg", "base_wind_u_kmh", "base_wind_v_kmh")
    return out
