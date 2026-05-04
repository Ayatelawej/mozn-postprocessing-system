from __future__ import annotations

import numpy as np
import pandas as pd

from postprocessing.features.circular import circular_diff_deg


def add_wind_direction_residual(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    station_col = "wind_direction_deg"
    base_col = "base_wind_direction_deg"
    if station_col not in out.columns or base_col not in out.columns:
        return out

    residual_deg = circular_diff_deg(out[station_col], out[base_col])
    out["winddir_residual_deg"] = residual_deg

    radians = np.deg2rad(residual_deg)
    out["winddir_residual_sin"] = np.sin(radians)
    out["winddir_residual_cos"] = np.cos(radians)
    return out
