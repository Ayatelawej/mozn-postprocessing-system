from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel
import os


class DataPaths(BaseModel):
    raw_dir: Path
    raw_station_dir: Path
    raw_metadata_dir: Path
    external_dir: Path
    external_openmeteo_dir: Path
    external_reference_dir: Path
    interim_dir: Path
    interim_stations_clean_dir: Path
    interim_aligned_dir: Path
    processed_dir: Path
    processed_train_tables_dir: Path
    processed_inference_tables_dir: Path
    manifests_dir: Path
    predictions_dir: Path


class ModelPaths(BaseModel):
    artifacts_dir: Path
    manifests_dir: Path


class ReportPaths(BaseModel):
    diagnostics_dir: Path
    figures_dir: Path


class NotebookPaths(BaseModel):
    reference_dir: Path
    diagnostics_dir: Path
    validation_dir: Path


class DocPaths(BaseModel):
    data_dir: Path


class ProjectPaths(BaseModel):
    project_root: Path
    data: DataPaths
    models: ModelPaths
    reports: ReportPaths
    notebooks: NotebookPaths
    docs: DocPaths
    config_dir: Path
    log_dir: Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve(root: Path, raw: str) -> Path:
    candidate = Path(raw)
    if candidate.is_absolute():
        return candidate
    return (root / candidate).resolve()


@lru_cache(maxsize=1)
def get_paths() -> ProjectPaths:
    root = _project_root()
    load_dotenv(root / ".env", override=False)

    paths_yaml = root / "configs" / "paths.yaml"
    with paths_yaml.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    env_root = os.getenv("PROJECT_ROOT")
    resolved_root = Path(env_root).resolve() if env_root else root

    def section(key: str) -> dict[str, Path]:
        return {k: _resolve(resolved_root, v) for k, v in raw[key].items()}

    env_log_dir = os.getenv("LOG_DIR")
    log_dir = (
        _resolve(resolved_root, env_log_dir)
        if env_log_dir
        else resolved_root / "logs"
    )

    return ProjectPaths(
        project_root=resolved_root,
        data=DataPaths(**section("data")),
        models=ModelPaths(**section("models")),
        reports=ReportPaths(**section("reports")),
        notebooks=NotebookPaths(**section("notebooks")),
        docs=DocPaths(**section("docs")),
        config_dir=resolved_root / "configs",
        log_dir=log_dir,
    )
