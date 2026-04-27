from postprocessing.utils.config import (
    AppConfig,
    DeploymentConfig,
    InferenceConfig,
    LoggingConfig,
    ModelsConfig,
    SchemaConfig,
    StationColumnMapping,
    StationsConfig,
    TrainingConfig,
    ValidationRulesConfig,
    get_config,
)


def test_get_config_returns_app_config_instance() -> None:
    config = get_config()
    assert isinstance(config, AppConfig)


def test_all_sections_load_with_correct_types() -> None:
    config = get_config()
    assert isinstance(config.schema_, SchemaConfig)
    assert isinstance(config.stations, StationsConfig)
    assert isinstance(config.validation_rules, ValidationRulesConfig)
    assert isinstance(config.models, ModelsConfig)
    assert isinstance(config.training, TrainingConfig)
    assert isinstance(config.inference, InferenceConfig)
    assert isinstance(config.deployment, DeploymentConfig)
    assert isinstance(config.logging, LoggingConfig)
    assert isinstance(config.station_column_mapping, StationColumnMapping)


def test_schema_required_columns_present() -> None:
    config = get_config()
    assert "station_id" in config.schema_.raw_station_required_columns
    assert "timestamp_utc" in config.schema_.raw_station_required_columns
    assert "temperature_c" in config.schema_.raw_station_optional_columns
    assert "pressure_max_hpa" in config.schema_.raw_station_optional_columns


def test_canonical_targets_have_expected_families() -> None:
    config = get_config()
    targets = config.schema_.canonical_targets
    assert "core" in targets
    assert "experimental" in targets
    assert "specialized" in targets
    assert "derived" in targets
    assert "temperature" in targets["core"]
    assert "wind_direction" in targets["specialized"]


def test_validation_rules_warmup_is_seven_days() -> None:
    config = get_config()
    assert config.validation_rules.warmup["default_days"] == 7


def test_validation_rules_have_all_targets() -> None:
    config = get_config()
    expected = {
        "temperature",
        "relative_humidity",
        "dew_point",
        "wind_speed",
        "uv",
        "pressure",
        "wind_gust",
        "rain_occurrence",
        "rain_amount",
        "wind_direction",
    }
    assert set(config.validation_rules.target_columns.keys()) == expected


def test_models_v1_subset_targets_match_target_model_map() -> None:
    config = get_config()
    for target in config.models.target_model_map:
        if config.models.target_model_map[target]:
            assert target in {
                "temperature",
                "relative_humidity",
                "dew_point",
                "wind_speed",
                "uv",
                "pressure",
                "wind_direction",
                "rain_occurrence",
                "rain_amount",
            }


def test_lstm_hyperparameters_present() -> None:
    config = get_config()
    lstm = config.models.sequence_models["lstm"]
    assert lstm["hidden_size"] == 96
    assert lstm["num_layers"] == 2
    assert lstm["lookback_hours"] == 48
    assert lstm["output_horizons"] == 72


def test_training_v1_subset_locked_decisions() -> None:
    config = get_config()
    subset = config.training.v1_training_subset
    assert "lstm" in subset["core_residual"]["models"]
    assert subset["experimental_gust"]["models"] == []


def test_deployment_api_port_matches_env_example() -> None:
    config = get_config()
    assert config.deployment.api_port == 8000
    assert config.deployment.api_host == "0.0.0.0"


def test_logging_uses_json_in_production() -> None:
    config = get_config()
    assert config.logging.json_logging is True
    assert config.logging.handlers["file"]["format"] == "json"


def test_station_column_mapping_handles_pressure() -> None:
    config = get_config()
    assert "pressure_max_hpa" in config.station_column_mapping.ordered_columns
    assert "pressure_min_hpa" in config.station_column_mapping.ordered_columns
    assert "pressure_trend_hpa" in config.station_column_mapping.ordered_columns


def test_config_caching_returns_same_instance() -> None:
    a = get_config()
    b = get_config()
    assert a is b
