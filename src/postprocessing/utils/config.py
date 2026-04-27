from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict

from postprocessing.utils.paths import get_paths


class SchemaConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    dataset_contract: dict[str, Any]
    raw_station_required_columns: list[str]
    raw_station_optional_columns: list[str]
    baseline_required_columns: list[str]
    baseline_optional_columns: list[str]
    canonical_train_table_required_columns: list[str]
    canonical_feature_groups: list[str]
    canonical_targets: dict[str, list[str]]


class StationsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    station_defaults: dict[str, Any]
    station_overrides: dict[str, Any]


class ValidationRulesConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    version: int
    warmup: dict[str, Any]
    activity_reference: dict[str, Any]
    global_required_columns: list[str]
    target_columns: dict[str, list[str]]
    hard_range_rules: dict[str, dict[str, Any]]
    advisory_bands: dict[str, dict[str, Any]] | None = None


class ModelsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    classical_models: dict[str, dict[str, Any]]
    sequence_models: dict[str, dict[str, Any]]
    experimental_models: dict[str, Any]
    target_model_map: dict[str, list[str]]
    acceptance_thresholds: dict[str, dict[str, Any]]
    selection_policy_notes: list[str] | None = None


class TrainingConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    validation: dict[str, Any]
    feature_policy: dict[str, Any]
    target_routing: dict[str, list[str]]
    models: dict[str, list[str]]
    selection_policy: dict[str, Any]
    v1_training_subset: dict[str, dict[str, Any]]


class InferenceConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    runtime: dict[str, Any]
    output: dict[str, Any]
    serving: dict[str, Any]


class DeploymentConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    api_host: str
    api_port: int
    container_image: dict[str, Any]
    staging_settings: dict[str, Any]
    production_settings: dict[str, Any]
    health_checks: dict[str, Any]
    resource_limits: dict[str, Any]


class LoggingConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    log_level: str
    log_dir: str
    json_logging: bool
    handlers: dict[str, Any]
    fields: dict[str, Any]


class StationColumnMapping(BaseModel):
    model_config = ConfigDict(extra="allow")

    version: int
    rename_map: dict[str, str]
    ordered_columns: list[str]


class AppConfig(BaseModel):
    schema_: SchemaConfig
    stations: StationsConfig
    validation_rules: ValidationRulesConfig
    models: ModelsConfig
    training: TrainingConfig
    inference: InferenceConfig
    deployment: DeploymentConfig
    logging: LoggingConfig
    station_column_mapping: StationColumnMapping


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if data is None:
        raise ValueError(f"Config file is empty: {path}")
    return data


@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    paths = get_paths()
    config_dir = paths.config_dir

    return AppConfig(
        schema_=SchemaConfig(**_load_yaml(config_dir / "schema.yaml")),
        stations=StationsConfig(**_load_yaml(config_dir / "stations.yaml")),
        validation_rules=ValidationRulesConfig(
            **_load_yaml(config_dir / "validation" / "station_validation_rules.yaml")
        ),
        models=ModelsConfig(**_load_yaml(config_dir / "models.yaml")),
        training=TrainingConfig(**_load_yaml(config_dir / "training.yaml")),
        inference=InferenceConfig(**_load_yaml(config_dir / "inference.yaml")),
        deployment=DeploymentConfig(**_load_yaml(config_dir / "deployment.yaml")),
        logging=LoggingConfig(**_load_yaml(config_dir / "logging.yaml")),
        station_column_mapping=StationColumnMapping(
            **_load_yaml(config_dir / "mappings" / "station_raw_column_mapping_v1.yaml")
        ),
    )
