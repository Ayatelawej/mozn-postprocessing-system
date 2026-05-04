from __future__ import annotations

import numpy as np
import pandas as pd

RAIN_OCCURRENCE_THRESHOLD_MM = 0.1


def add_rain_targets(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "rain_total_mm" not in out.columns:
        return out

    rain_mm = out["rain_total_mm"]

    occurrence = (rain_mm > RAIN_OCCURRENCE_THRESHOLD_MM).astype("int8")
    occurrence = occurrence.where(rain_mm.notna(), other=pd.NA)
    out["rain_occurrence"] = occurrence

    out["rain_amount_log1p"] = np.log1p(rain_mm.clip(lower=0.0))
    out["rain_amount_mm"] = rain_mm
    return out
