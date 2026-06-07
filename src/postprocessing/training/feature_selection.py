from __future__ import annotations


SHARED_METADATA: tuple[str, ...] = (
    "elevation_m", "station_latitude", "station_longitude",
)

LAG_OFFSETS: tuple[int, ...] = (1, 2, 3, 6, 12, 24)
ROLL_WINDOWS: tuple[int, ...] = (3, 6, 12, 24)


def _lags(var: str, offsets: tuple[int, ...] = LAG_OFFSETS) -> tuple[str, ...]:
    return tuple(f"{var}_lag_{h}h" for h in offsets)


def _rolls(var: str, windows: tuple[int, ...] = ROLL_WINDOWS) -> tuple[str, ...]:
    return tuple(
        f"{var}_roll_{stat}_{w}h"
        for w in windows for stat in ("mean", "std")
    )


ISSUE_TIME_WIND_CIRCULAR: tuple[str, ...] = (
    "station_wind_u_kmh", "station_wind_v_kmh",
    "station_wind_direction_sin", "station_wind_direction_cos",
)

VALIDITY_TIME_TIME: tuple[str, ...] = ("hour_sin", "hour_cos", "is_day_flag")

VALIDITY_TIME_SOLAR_DERIVED: tuple[str, ...] = (
    "minutes_from_sunrise", "solar_progress_0_1", "solar_centered",
)

VALIDITY_TIME_RADIATION_DERIVED: tuple[str, ...] = (
    "clipped_shortwave_wm2", "cloud_attenuation_factor",
    "sunshine_fraction", "uv_proxy", "solar_to_clear_sky_ratio",
)


FEATURE_SPECS: dict[str, dict[str, tuple[str, ...]]] = {
    "temperature": {
        "issue_time": (
            "temperature_residual_c",
            *SHARED_METADATA,
            *_lags("temperature_c"),
            *_lags("relative_humidity_pct"),
            *_lags("dew_point_c"),
            *_rolls("temperature_c"),
        ),
        "validity_time": (
            "base_temperature_c", "base_relative_humidity_pct", "base_dew_point_c",
            "base_cloud_cover_pct", "base_is_day",
            *VALIDITY_TIME_TIME,
            *VALIDITY_TIME_SOLAR_DERIVED,
            *VALIDITY_TIME_RADIATION_DERIVED,
        ),
    },
    "relative_humidity": {
        "issue_time": (
            "relative_humidity_residual_pct",
            *SHARED_METADATA,
            *_lags("temperature_c"),
            *_lags("relative_humidity_pct"),
            *_lags("dew_point_c"),
            *_rolls("relative_humidity_pct"),
        ),
        "validity_time": (
            "base_temperature_c", "base_relative_humidity_pct", "base_dew_point_c",
            "base_cloud_cover_pct", "base_is_day",
            *VALIDITY_TIME_TIME,
            *VALIDITY_TIME_SOLAR_DERIVED,
        ),
    },
    "dew_point": {
        "issue_time": (
            "dew_point_residual_c",
            *SHARED_METADATA,
            *_lags("temperature_c"),
            *_lags("relative_humidity_pct"),
            *_lags("dew_point_c"),
            *_rolls("dew_point_c"),
        ),
        "validity_time": (
            "base_temperature_c", "base_relative_humidity_pct", "base_dew_point_c",
            "base_cloud_cover_pct",
            *VALIDITY_TIME_TIME,
        ),
    },
    "wind_speed": {
        "issue_time": (
            "wind_speed_residual_kmh",
            *SHARED_METADATA,
            *_lags("wind_speed_kmh"),
            *_rolls("wind_speed_kmh"),
            *ISSUE_TIME_WIND_CIRCULAR,
        ),
        "validity_time": (
            "base_wind_speed_kmh", "base_wind_gust_kmh",
            "base_wind_u_kmh", "base_wind_v_kmh",
            "base_wind_direction_sin", "base_wind_direction_cos",
            "base_msl_pressure_hpa", "base_cloud_cover_pct",
            *VALIDITY_TIME_TIME,
        ),
    },
    "wind_gust": {
        "issue_time": (
            "wind_gust_residual_kmh",
            *SHARED_METADATA,
            *_lags("wind_speed_kmh"),
            *_rolls("wind_speed_kmh"),
            *ISSUE_TIME_WIND_CIRCULAR,
        ),
        "validity_time": (
            "base_wind_speed_kmh", "base_wind_gust_kmh",
            "base_wind_u_kmh", "base_wind_v_kmh",
            "base_msl_pressure_hpa", "base_cloud_cover_pct",
            *VALIDITY_TIME_TIME,
        ),
    },
    "pressure": {
        "issue_time": (
            "pressure_residual_max_hpa",
            *SHARED_METADATA,
            *_lags("pressure_max_hpa"),
            *_rolls("pressure_max_hpa"),
            *_lags("temperature_c"),
        ),
        "validity_time": (
            "base_msl_pressure_hpa", "base_temperature_c",
            "base_cloud_cover_pct",
            *VALIDITY_TIME_TIME,
        ),
    },
    "wind_direction": {
        "issue_time": (
            "winddir_residual_sin", "winddir_residual_cos",
            *SHARED_METADATA,
            *_lags("wind_speed_kmh"),
            *ISSUE_TIME_WIND_CIRCULAR,
            "wind_speed_kmh",
        ),
        "validity_time": (
            "base_wind_speed_kmh",
            "base_wind_u_kmh", "base_wind_v_kmh",
            "base_wind_direction_sin", "base_wind_direction_cos",
            "base_msl_pressure_hpa",
            *VALIDITY_TIME_TIME,
        ),
    },
    "rain_occurrence": {
        "issue_time": (
            "station_rain_event",
            "station_rain_rolling_3h_mm", "station_rain_rolling_6h_mm",
            *SHARED_METADATA,
            *_lags("rain_total_mm"),
            *_rolls("rain_total_mm"),
            *_lags("relative_humidity_pct"),
            *_lags("pressure_max_hpa"),
        ),
        "validity_time": (
            "base_precipitation_mm", "base_rain_total_mm",
            "base_rain_rolling_3h_mm", "base_rain_rolling_6h_mm",
            "base_cloud_cover_pct", "base_cloud_cover_low_pct",
            "base_msl_pressure_hpa", "base_relative_humidity_pct",
            *VALIDITY_TIME_TIME,
        ),
    },
    "rain_amount": {
        "issue_time": (
            *SHARED_METADATA,
            *_lags("rain_total_mm"),
            *_rolls("rain_total_mm"),
            *_lags("relative_humidity_pct"),
            "station_rain_rolling_3h_mm", "station_rain_rolling_6h_mm",
        ),
        "validity_time": (
            "base_precipitation_mm", "base_rain_total_mm",
            "base_rain_rolling_3h_mm", "base_rain_rolling_6h_mm",
            "base_cloud_cover_pct",
            "base_msl_pressure_hpa", "base_relative_humidity_pct",
            *VALIDITY_TIME_TIME,
        ),
    },
    "uv": {
        "issue_time": (
            "uv_index",
            *SHARED_METADATA,
        ),
        "validity_time": (
            "base_solar_radiation_wm2", "base_direct_radiation_wm2",
            "base_diffuse_radiation_wm2", "base_sunshine_seconds",
            "base_cloud_cover_pct", "base_cloud_cover_low_pct",
            "base_cloud_cover_mid_pct", "base_cloud_cover_high_pct",
            "base_is_day",
            *VALIDITY_TIME_TIME,
            *VALIDITY_TIME_SOLAR_DERIVED,
            *VALIDITY_TIME_RADIATION_DERIVED,
        ),
    },
}


def known_targets() -> tuple[str, ...]:
    return tuple(FEATURE_SPECS.keys())


def features_for(target: str, lead: int) -> list[str]:
    if target not in FEATURE_SPECS:
        raise KeyError(
            f"Unknown target '{target}'. Known: {known_targets()}"
        )
    if lead < 1:
        raise ValueError(f"lead must be >= 1, got {lead}")
    spec = FEATURE_SPECS[target]
    issue_time = list(spec["issue_time"])
    validity_time = [f"{col}_lead_{lead}h" for col in spec["validity_time"]]
    return issue_time + validity_time


def audit_feature_lists() -> None:
    for target in FEATURE_SPECS:
        for lead in (1, 12, 72):
            features = features_for(target, lead)
            for col in features:
                if col == "station_id":
                    raise AssertionError(
                        f"station_id must not be in feature list for {target} "
                        f"(lead={lead}); decision #13"
                    )
                if "pressure_trend_hpa" in col:
                    raise AssertionError(
                        f"pressure_trend_hpa-derived column in feature list for "
                        f"{target} (lead={lead}): {col}; decision #16"
                    )
    for wind_target in ("wind_speed", "wind_gust"):
        features = features_for(wind_target, 1)
        if "elevation_m" not in features:
            raise AssertionError(
                f"elevation_m is mandatory in feature list for {wind_target}; "
                f"decision #18"
            )


audit_feature_lists()
