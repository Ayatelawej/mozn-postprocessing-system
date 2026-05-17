from __future__ import annotations

from postprocessing.training.data_loader import load_canonical
from postprocessing.training.ridge_runner import (
    save_artifact,
    save_result_json,
    train_ridge,
)
from postprocessing.utils.paths import get_paths


HOLDOUT_STATION = "IDERNA7"
TARGET = "temperature"
LEAD = 1
ALPHA = 1.0


def main() -> None:
    paths = get_paths()
    canonical_path = paths.data.processed_dir / "canonical_hourly_v1.parquet"
    print(f"Loading canonical: {canonical_path}")
    df = load_canonical(canonical_path)
    print(f"  Loaded: {len(df):,} rows, {df['station_id'].nunique()} stations")
    print()
    print(f"Training Ridge: target={TARGET}, lead={LEAD}h, holdout={HOLDOUT_STATION}, alpha={ALPHA}")
    print()

    result, artifact = train_ridge(df, TARGET, LEAD, HOLDOUT_STATION, alpha=ALPHA)

    print("=== Training data ===")
    print(f"  Train rows:       {result.n_train:,}")
    print(f"  Val rows:         {result.n_val:,}")
    print(f"  Features used:    {len(result.feature_columns)}")
    print(f"  Features missing: {len(result.missing_features)}")
    if result.missing_features:
        print(f"  Missing list:")
        for m in result.missing_features:
            print(f"    - {m}")
    print()
    print("=== Baseline (Open-Meteo alone) ===")
    print(f"  MAE:  {result.baseline_mae:.4f} C")
    print(f"  RMSE: {result.baseline_rmse:.4f} C")
    print()
    print("=== Corrected (Ridge applied) ===")
    print(f"  MAE:  {result.corrected_mae:.4f} C")
    print(f"  RMSE: {result.corrected_rmse:.4f} C")
    print()
    print("=== Bias reduction ===")
    print(f"  MAE reduction:  {result.mae_reduction_pct:+.2f}%")
    print(f"  RMSE reduction: {result.rmse_reduction_pct:+.2f}%")
    print()

    artifact_path = paths.models.artifacts_dir / f"{TARGET}_ridge_lead{LEAD}_loso_{HOLDOUT_STATION}.joblib"
    result_path = paths.reports.diagnostics_dir / "task2_ridge_smoke" / f"{TARGET}_ridge_lead{LEAD}_loso_{HOLDOUT_STATION}.json"
    save_artifact(artifact, artifact_path)
    save_result_json(result, result_path)
    print(f"Artifact:    {artifact_path}")
    print(f"Result JSON: {result_path}")


if __name__ == "__main__":
    main()
