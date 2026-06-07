from __future__ import annotations

import numpy as np

from postprocessing.training.data_loader import load_canonical, target_columns_for
from postprocessing.training.preparation import prepare_for_target
from postprocessing.training.ridge_runner import baseline_column_for


TARGETS_TO_CHECK: tuple[str, ...] = ("temperature", "pressure", "wind_gust")
LEAD: int = 1


def main():
    print("Loading canonical")
    df = load_canonical()
    print()

    for target in TARGETS_TO_CHECK:
        print(f"=== {target} (lead={LEAD}) ===")
        framed = prepare_for_target(df, target, LEAD)
        target_col = f"{target_columns_for(target)[0]}_lead_{LEAD}h"
        baseline_col = baseline_column_for(target)
        baseline_lead_col = f"{baseline_col}_lead_{LEAD}h"

        residual_lead = framed[target_col].to_numpy()
        baseline_lead = framed[baseline_lead_col].to_numpy() if baseline_lead_col in framed.columns else None

        keep = ~np.isnan(residual_lead)
        residual_clean = residual_lead[keep]

        mae_from_residual = float(np.mean(np.abs(residual_clean)))
        bias_from_residual = float(np.mean(residual_clean))

        print(f"  Target column:              {target_col}")
        print(f"  Baseline column:            {baseline_col}")
        print(f"  Sample size after dropna:   {len(residual_clean):,}")
        print(f"  Residual mean (bias):       {bias_from_residual:+.4f}")
        print(f"  Residual MAE (baseline):    {mae_from_residual:.4f}")

        sample_idx = np.where(keep)[0][:5]
        print(f"  First 5 rows (issue_time, station, baseline_at_T+1, residual_at_T+1):")
        for i in sample_idx:
            issue_t = framed["issue_time_utc"].iloc[i]
            sid = framed["station_id"].iloc[i]
            baseline_at_lead = framed[baseline_lead_col].iloc[i] if baseline_lead_col in framed.columns else None
            res_at_lead = framed[target_col].iloc[i]
            print(f"    {issue_t}  {sid:>10}  base_lead={baseline_at_lead:.3f}  resid_lead={res_at_lead:+.4f}")
        print()


if __name__ == "__main__":
    main()
