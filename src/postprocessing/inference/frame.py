from __future__ import annotations

import pandas as pd

from postprocessing.horizon.framing import build_horizon_table
from postprocessing.training.data_loader import VALIDITY_TIME_LEAD_COLUMNS


INFERENCE_LEADS = tuple(range(1, 73))


def _as_utc_ts(ts):
    out = pd.Timestamp(ts)
    if out.tzinfo is None:
        return out.tz_localize("UTC")
    return out.tz_convert("UTC")


def build_inference_frame(canonical_style, issue_time, leads=INFERENCE_LEADS):
    if "valid_time_utc" not in canonical_style.columns:
        raise KeyError("canonical_style must contain 'valid_time_utc'")
    if "station_id" not in canonical_style.columns:
        raise KeyError("canonical_style must contain 'station_id'")

    cs = canonical_style.copy()
    if not pd.api.types.is_datetime64_any_dtype(cs["valid_time_utc"]):
        cs["valid_time_utc"] = pd.to_datetime(cs["valid_time_utc"], utc=True)
    elif cs["valid_time_utc"].dt.tz is None:
        cs["valid_time_utc"] = cs["valid_time_utc"].dt.tz_localize("UTC")

    cols_to_lead = [c for c in VALIDITY_TIME_LEAD_COLUMNS if c in cs.columns]
    framed = build_horizon_table(cs, cols_to_lead, leads, drop_incomplete=False)

    issue_ts = _as_utc_ts(issue_time)
    rows = framed[framed["issue_time_utc"] == issue_ts].reset_index(drop=True)
    return rows
