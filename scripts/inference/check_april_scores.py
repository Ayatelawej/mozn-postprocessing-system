from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from postprocessing.inference.reconstruct import angle_from_sin_cos, correct
from postprocessing.training.artifact_training import load_artifact, predict_with_artifact
from postprocessing.training.data_loader import predicts_residual, target_columns_for
from postprocessing.training.preparation import prepare_for_target
from postprocessing.training.ridge_runner import BASELINE_COLUMN


APRIL_PATH = r"data/processed/april2026_eval_frame_v3.parquet"
METRICS_PATH = r"reports/block2_final/master_metrics.csv"
ARTIFACT_DIR = r"models/artifacts"
LEADS = (1, 6, 24, 48, 72)
MODEL_OF = {
    "temperature": "HGB",
    "relative_humidity": "HGB",
    "dew_point": "HGB",
    "wind_speed": "Ridge",
    "wind_gust": "Ridge",
    "pressure": "Ridge",
    "uv": "HGB",
    "wind_direction": "Ridge",
}
RED_TOL_PP = 0.25
MAE_REL_TOL = 0.005
RECON_TOL = 1e-9


def circular_mae(pred_deg, actual_deg):
    d = np.mod(pred_deg - actual_deg + 180.0, 360.0) - 180.0
    return float(np.mean(np.abs(d)))


def get_oracle(metrics, target, lead):
    m = metrics[
        (metrics["validation_mode"] == "april")
        & (metrics["target"] == target)
        & (metrics["lead_hours"] == float(lead))
    ]
    return m.iloc[0]


def main():
    april = pd.read_parquet(APRIL_PATH)
    april["valid_time_utc"] = pd.to_datetime(april["valid_time_utc"], utc=True)
    metrics = pd.read_csv(METRICS_PATH)

    header = f"{'target':18}{'lead':>5}{'metric':>12}{'computed':>12}{'oracle':>12}{'delta':>11}{'n comp/orc':>16}{'recon':>7}  status"
    print(header)
    all_ok = True

    for target, model_class in MODEL_OF.items():
        for lead in LEADS:
            art = load_artifact(f"{ARTIFACT_DIR}/{target}_{model_class}_lead{lead}h.joblib")
            eval_df = prepare_for_target(april, target, lead)
            pred, keep = predict_with_artifact(art, eval_df)
            corrected_full, keep2 = correct(art, eval_df, clamp=False)
            recon_ok = bool(np.array_equal(keep, keep2))
            orc = get_oracle(metrics, target, lead)
            n_orc = int(orc["n_eval"])

            if target == "wind_direction":
                bsin = eval_df[f"base_wind_direction_sin_lead_{lead}h"].to_numpy(dtype=float)
                bcos = eval_df[f"base_wind_direction_cos_lead_{lead}h"].to_numpy(dtype=float)
                ysin = eval_df[f"winddir_residual_sin_lead_{lead}h"].to_numpy(dtype=float)
                ycos = eval_df[f"winddir_residual_cos_lead_{lead}h"].to_numpy(dtype=float)
                pred_resid = np.degrees(np.arctan2(pred[:, 0], pred[:, 1]))
                ref_full = np.full(len(eval_df), np.nan)
                ref_full[keep] = np.mod(angle_from_sin_cos(bsin[keep], bcos[keep]) + pred_resid, 360.0)
                m = keep & ~np.isnan(ysin) & ~np.isnan(ycos) & ~np.isnan(bsin) & ~np.isnan(bcos)
                base_angle = angle_from_sin_cos(bsin[m], bcos[m])
                actual_angle = np.mod(base_angle + angle_from_sin_cos(ysin[m], ycos[m]), 360.0)
                base_mae = circular_mae(base_angle, actual_angle)
                corr_mae = circular_mae(ref_full[m], actual_angle)
                computed = 100.0 * (base_mae - corr_mae) / base_mae
                oracle = float(orc["mae_reduction_pct"])
                metric = "circ_red%"
                recon_ok = recon_ok and bool(np.allclose(corrected_full[m], ref_full[m], atol=RECON_TOL))
                pass_metric = abs(computed - oracle) <= RED_TOL_PP

            elif not predicts_residual(target):
                tcol = f"{target_columns_for(target)[0]}_lead_{lead}h"
                y = eval_df[tcol].to_numpy(dtype=float)
                bvals = eval_df[f"{BASELINE_COLUMN[target]}_lead_{lead}h"].to_numpy(dtype=float)
                pred_full = np.full(len(eval_df), np.nan)
                pred_full[keep] = pred
                m = keep & ~np.isnan(y) & ~np.isnan(bvals)
                corr_mae = float(np.mean(np.abs(pred_full[m] - y[m])))
                computed = corr_mae
                oracle = float(orc["absolute_mae"])
                metric = "uv_abs_mae"
                recon_ok = recon_ok and bool(np.allclose(corrected_full[m], pred_full[m], atol=RECON_TOL))
                pass_metric = abs(computed - oracle) <= max(MAE_REL_TOL * oracle, 1e-6)

            else:
                tcol = f"{target_columns_for(target)[0]}_lead_{lead}h"
                y = eval_df[tcol].to_numpy(dtype=float)
                base_lead = eval_df[f"{BASELINE_COLUMN[target]}_lead_{lead}h"].to_numpy(dtype=float)
                pred_full = np.full(len(eval_df), np.nan)
                pred_full[keep] = pred
                m = keep & ~np.isnan(y)
                base_mae = float(np.mean(np.abs(y[m])))
                corr_mae = float(np.mean(np.abs(y[m] - pred_full[m])))
                computed = 100.0 * (base_mae - corr_mae) / base_mae
                oracle = float(orc["mae_reduction_pct"])
                metric = "mae_red%"
                ref_full = np.full(len(eval_df), np.nan)
                ref_full[keep] = base_lead[keep] + pred
                mb = m & ~np.isnan(base_lead)
                recon_ok = recon_ok and bool(np.allclose(corrected_full[mb], ref_full[mb], atol=RECON_TOL))
                pass_metric = abs(computed - oracle) <= RED_TOL_PP

            n_comp = int(m.sum())
            status = "OK" if pass_metric and recon_ok and n_comp == n_orc else "FAIL"
            all_ok = all_ok and status == "OK"
            print(f"{target:18}{lead:>5}{metric:>12}{computed:>12.4f}{oracle:>12.4f}{computed - oracle:>+11.4f}{f'{n_comp}/{n_orc}':>16}{('ok' if recon_ok else 'BAD'):>7}  {status}")

    print(f"\nSCORE REPRODUCTION {'PASS' if all_ok else 'FAIL'}")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
