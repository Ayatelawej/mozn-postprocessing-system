from __future__ import annotations

import numpy as np

from postprocessing.training.artifact_training import predict_with_artifact


PHYSICAL_CLAMPS = {
    "relative_humidity": (0.0, 100.0),
    "wind_speed": (0.0, None),
    "wind_gust": (0.0, None),
    "pressure": (870.0, 1085.0),
    "uv": (0.0, 16.0),
}


def angle_from_sin_cos(sin_vals, cos_vals):
    return np.mod(np.degrees(np.arctan2(sin_vals, cos_vals)), 360.0)


def apply_physical_clamps(values, target):
    bounds = PHYSICAL_CLAMPS.get(target)
    if bounds is None:
        return values
    lo, hi = bounds
    out = values
    if lo is not None:
        out = np.where(out < lo, lo, out)
    if hi is not None:
        out = np.where(out > hi, hi, out)
    return out


def correct(artifact, frame, *, clamp=True):
    target = artifact["target"]
    lead = artifact["lead"]
    pred, keep = predict_with_artifact(artifact, frame)
    out = np.full(len(frame), np.nan)
    if keep.sum() == 0:
        return out, keep

    if target == "wind_direction":
        base_sin = frame[f"base_wind_direction_sin_lead_{lead}h"].to_numpy(dtype=float)[keep]
        base_cos = frame[f"base_wind_direction_cos_lead_{lead}h"].to_numpy(dtype=float)[keep]
        base_angle = angle_from_sin_cos(base_sin, base_cos)
        resid_angle = np.degrees(np.arctan2(pred[:, 0], pred[:, 1]))
        out[keep] = np.mod(base_angle + resid_angle, 360.0)
        return out, keep

    if artifact["predicts_residual"]:
        baseline_lead = frame[f"{artifact['baseline_column']}_lead_{lead}h"].to_numpy(dtype=float)[keep]
        corrected = baseline_lead + pred
    else:
        corrected = pred

    if clamp:
        corrected = apply_physical_clamps(corrected, target)
    out[keep] = corrected
    return out, keep
