from __future__ import annotations

import time

from postprocessing.training.artifact_training import (
    artifact_path,
    load_artifact,
    predict_with_artifact,
    save_artifact,
    train_scalar_target_artifact,
    train_wind_direction_artifact,
)
from postprocessing.training.data_loader import load_canonical
from postprocessing.training.preparation import prepare_for_target


WINNING_CONFIG = {
    "temperature":       {"model": "HGB",   "hp": {"max_depth": 10, "learning_rate": 0.1}},
    "relative_humidity": {"model": "HGB",   "hp": {"max_depth": 10, "learning_rate": 0.1}},
    "dew_point":         {"model": "HGB",   "hp": {"max_depth": 10, "learning_rate": 0.1}},
    "wind_speed":        {"model": "Ridge", "hp": {"alpha": 10.0}},
    "wind_gust":         {"model": "Ridge", "hp": {"alpha": 0.1}},
    "pressure":          {"model": "Ridge", "hp": {"alpha": 0.1}},
    "uv":                {"model": "HGB",   "hp": {"max_depth": 10, "learning_rate": 0.1}},
    "wind_direction":    {"model": "Ridge", "hp": {"alpha": 10.0}},
}


def main():
    print("Loading canonical")
    canonical = load_canonical()
    print(f"  canonical: {canonical.shape}")
    print()

    lead = 1
    print(f"=== Smoke test: train + save + load + predict at lead={lead}h ===")
    print()

    total_start = time.time()
    artifact_paths = {}

    for target, config in WINNING_CONFIG.items():
        model_class = config["model"]
        hp = config["hp"]
        t0 = time.time()
        try:
            if target == "wind_direction":
                artifact = train_wind_direction_artifact(canonical, lead, hp)
            else:
                artifact = train_scalar_target_artifact(canonical, target, lead, model_class, hp)
            path = artifact_path(target, lead, model_class)
            save_artifact(artifact, path)
            elapsed = time.time() - t0
            size_kb = path.stat().st_size / 1024
            artifact_paths[target] = path
            print(f"  {target:>20} ({model_class:>5})  trained n={artifact['n_train']:>6,}  "
                  f"features={len(artifact['feature_columns']):>3}  "
                  f"size={size_kb:>7.1f}KB  time={elapsed:>4.1f}s")
        except Exception as e:
            print(f"  {target:>20} ({model_class:>5})  ERROR: {type(e).__name__}: {e}")

    print()
    print(f"All {len(artifact_paths)} artifacts trained and saved.")
    print(f"Train+save time: {time.time() - total_start:.1f}s")
    print()

    print("=== Verify each artifact loads and predicts on a small sample ===")
    print()
    for target, path in artifact_paths.items():
        try:
            artifact = load_artifact(path)
            sample_frame = prepare_for_target(canonical, target, lead).head(100)
            preds, keep = predict_with_artifact(artifact, sample_frame)
            n_predicted = len(preds)
            sample_pred = preds[0] if n_predicted > 0 else None
            if target == "wind_direction":
                shape_str = f"shape={preds.shape}"
            else:
                shape_str = f"shape=({n_predicted},)"
            print(f"  {target:>20}  loaded ok  predicted on {n_predicted}/100 rows  "
                  f"{shape_str}  first_pred={sample_pred}")
        except Exception as e:
            print(f"  {target:>20}  VERIFICATION ERROR: {type(e).__name__}: {e}")

    print()
    print("=== Smoke complete ===")
    print("If everything above looks fine, kick off the full 72-lead training run (Task 15b).")


if __name__ == "__main__":
    main()
