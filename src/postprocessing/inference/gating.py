from __future__ import annotations

import numpy as np
import pandas as pd

from postprocessing.inference.reconstruct import angle_from_sin_cos, correct
from postprocessing.training.artifact_training import load_artifact

DARK_KEEP = {"temperature", "relative_humidity", "dew_point", "wind_speed", "wind_gust", "uv"}
DARK_FALLBACK = {"pressure", "wind_direction"}

RESIDUAL_CAP = {
    "temperature": 6.0,
    "dew_point": 9.0,
    "relative_humidity": 27.0,
    "wind_speed": 30.0,
    "wind_gust": 56.0,
    "pressure": 17.0,
}

SUFFICIENCY_THRESHOLD = 0.75
RECENCY_WINDOW_H = 24


def recent_obs_fraction(obs_df, issue_time, window_hours=RECENCY_WINDOW_H):
    t = pd.Timestamp(issue_time)
    t = t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")
    lo = t - pd.Timedelta(hours=window_hours - 1)
    win = obs_df[(obs_df["valid_time_utc"] >= lo) & (obs_df["valid_time_utc"] <= t)]
    out = {}
    for sid, g in win.groupby("station_id"):
        if "sample_count" in g.columns:
            real = int((g["sample_count"].fillna(0) > 0).sum())
        else:
            real = int(g["temperature_c"].notna().sum())
        out[sid] = real / window_hours
    return out


def fallback_value(target, artifact, row, lead):
    if target == "wind_direction":
        bs = row.get(f"base_wind_direction_sin_lead_{lead}h")
        bc = row.get(f"base_wind_direction_cos_lead_{lead}h")
        if bs is None or bc is None or pd.isna(bs) or pd.isna(bc):
            return np.nan
        return float(angle_from_sin_cos(np.array([float(bs)]), np.array([float(bc)]))[0])
    if not artifact["predicts_residual"]:
        return np.nan
    v = row.get(f"{artifact['baseline_column']}_lead_{lead}h")
    return float(v) if v is not None and not pd.isna(v) else np.nan


def decide_cell(target, lead, artifact, row, corrected_value, kept, sufficient, is_new):
    base_val = fallback_value(target, artifact, row, lead)

    if not kept or corrected_value is None or (isinstance(corrected_value, float) and np.isnan(corrected_value)):
        return base_val, "fallback", "structural_missing"

    if sufficient:
        value = corrected_value
        status, reason = "ok", ("new_station_sufficient" if is_new else "")
    elif is_new:
        return base_val, "fallback", "new_station_unvalidated"
    elif target in DARK_FALLBACK:
        return base_val, "fallback", "insufficient_recent_obs"
    else:
        value = corrected_value
        status, reason = "low_confidence", "insufficient_recent_obs"

    cap = RESIDUAL_CAP.get(target)
    if cap is not None and artifact["predicts_residual"] and not pd.isna(base_val):
        if abs(corrected_value - base_val) > cap:
            return base_val, "fallback", "output_qc_failed"
    return value, status, reason


def run_gated_inference(inf, resolutions, obs_df, issue_time, leads, models,
                        artifact_dir="models/artifacts", threshold=SUFFICIENCY_THRESHOLD):
    frac = recent_obs_fraction(obs_df, issue_time)
    is_new = dict(zip(resolutions["station_id"], resolutions["needs_openmeteo_elevation"].astype(bool)))
    wu = dict(zip(resolutions["station_id"], resolutions["matched_wu_id"]))
    out = []
    for target, model_class in models.items():
        for lead in leads:
            art = load_artifact(f"{artifact_dir}/{target}_{model_class}_lead{lead}h.joblib")
            corrected, keep = correct(art, inf, clamp=True)
            for i in range(len(inf)):
                row = inf.iloc[i]
                sid = row["station_id"]
                suff = frac.get(sid, 0.0) >= threshold
                val, status, reason = decide_cell(
                    target, lead, art, row,
                    float(corrected[i]) if keep[i] else np.nan, bool(keep[i]),
                    suff, is_new.get(sid, False),
                )
                out.append({"station_id": sid, "wu_id": wu.get(sid), "target": target, "lead": lead,
                            "value": val, "status": status, "reason": reason,
                            "recent_obs_frac": round(frac.get(sid, 0.0), 3)})
    return pd.DataFrame(out)
