from __future__ import annotations

import json
import time
from pathlib import Path

from postprocessing.training.artifact_training import (
    artifact_path,
    save_artifact,
    train_scalar_target_artifact,
    train_wind_direction_artifact,
)
from postprocessing.training.data_loader import load_canonical


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


LEADS = tuple(range(1, 73))


def train_one(canonical, target, lead, model_class, hp):
    if target == "wind_direction":
        return train_wind_direction_artifact(canonical, lead, hp)
    return train_scalar_target_artifact(canonical, target, lead, model_class, hp)


def main():
    print("Loading canonical")
    canonical = load_canonical()
    print(f"  canonical: {canonical.shape}")
    print()
    print("=== Full 72-lead sweep: 8 targets x 72 leads = 576 fits ===")
    print()

    log_dir = Path("reports/diagnostics/artifact_training")
    log_dir.mkdir(parents=True, exist_ok=True)
    progress_log = log_dir / "training_progress.tsv"
    progress_log.write_text(
        "target\tmodel_class\tlead\tn_train\tn_features\tartifact_size_kb\telapsed_sec\tstatus\terror\n",
        encoding="utf-8",
    )

    total_start = time.time()
    completed = 0
    failed = 0
    summary = {}

    for target, config in WINNING_CONFIG.items():
        model_class = config["model"]
        hp = config["hp"]
        target_start = time.time()
        target_completed = 0
        target_failed = 0
        print(f"\n--- {target} ({model_class}) ---")
        for lead in LEADS:
            t0 = time.time()
            try:
                artifact = train_one(canonical, target, lead, model_class, hp)
                path = artifact_path(target, lead, model_class)
                save_artifact(artifact, path)
                size_kb = path.stat().st_size / 1024
                elapsed = time.time() - t0
                completed += 1
                target_completed += 1
                with progress_log.open("a", encoding="utf-8") as f:
                    f.write(
                        f"{target}\t{model_class}\t{lead}\t{artifact['n_train']}\t"
                        f"{len(artifact['feature_columns'])}\t{size_kb:.1f}\t{elapsed:.2f}\tok\t\n"
                    )
                if lead % 12 == 1 or lead in (6, 24, 48, 72):
                    print(f"  lead={lead:>2}h  n={artifact['n_train']:>6,}  "
                          f"size={size_kb:>7.1f}KB  time={elapsed:>5.2f}s")
            except Exception as e:
                elapsed = time.time() - t0
                failed += 1
                target_failed += 1
                err_msg = f"{type(e).__name__}: {e}"
                with progress_log.open("a", encoding="utf-8") as f:
                    f.write(
                        f"{target}\t{model_class}\t{lead}\t\t\t\t{elapsed:.2f}\terror\t{err_msg}\n"
                    )
                print(f"  lead={lead:>2}h  ERROR: {err_msg}")

        target_elapsed = time.time() - target_start
        summary[target] = {
            "model_class": model_class,
            "hp": hp,
            "completed": target_completed,
            "failed": target_failed,
            "wall_time_sec": target_elapsed,
            "wall_time_min": target_elapsed / 60,
        }
        print(f"  ... {target} done in {target_elapsed:.1f}s "
              f"({target_completed} ok / {target_failed} failed)")

    total_elapsed = time.time() - total_start
    print()
    print(f"=== Full sweep complete in {total_elapsed:.1f}s ({total_elapsed/60:.1f} min, {total_elapsed/3600:.2f} hr) ===")
    print(f"Total fits: {completed} ok / {failed} failed / {completed + failed} attempted")
    print()
    print("Per-target wall time:")
    for target, s in summary.items():
        print(f"  {target:>20} ({s['model_class']:>5})  "
              f"ok={s['completed']:>2}/72  failed={s['failed']:>2}  "
              f"time={s['wall_time_min']:>5.1f} min")

    summary_path = log_dir / "training_summary.json"
    with summary_path.open("w") as f:
        json.dump({
            "total_fits": completed + failed,
            "completed": completed,
            "failed": failed,
            "wall_time_sec": total_elapsed,
            "per_target": summary,
        }, f, indent=2, default=str)
    print(f"\nSummary saved: {summary_path}")
    print(f"Progress log: {progress_log}")


if __name__ == "__main__":
    main()
