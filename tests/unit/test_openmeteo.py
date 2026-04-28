from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest

from postprocessing.ingestion.openmeteo import (
    DAILY_RENAME,
    DAILY_VARIABLES,
    HOURLY_RENAME,
    HOURLY_VARIABLES,
    OpenMeteoFetchError,
    _build_daily_dataframe,
    _build_hourly_dataframe,
    _has_complete_cache,
    fetch_station_baseline,
)
from postprocessing.ingestion.station_registry import Station


def make_test_station() -> Station:
    return Station(
        station_id="ITEST_FAKE",
        station_name="Test Station",
        city="Tripoli",
        country="Libya",
        latitude=32.88,
        longitude=13.16,
        elevation_m=10.0,
        install_date=datetime(2025, 6, 15, tzinfo=timezone.utc),
        pooled_first_timestamp=datetime(2025, 10, 9, 12, tzinfo=timezone.utc),
        pooled_last_timestamp=datetime(2025, 10, 11, 12, tzinfo=timezone.utc),
        pooled_rows=48,
        pooled_present_hours=48,
    )


def make_full_payload() -> dict[str, Any]:
    return {
        "hourly": {
            "time": [
                "2025-10-09T12:00",
                "2025-10-09T13:00",
                "2025-10-09T14:00",
            ],
            "temperature_2m": [20.1, 20.5, 21.0],
            "relative_humidity_2m": [55, 53, 51],
            "dew_point_2m": [10.0, 10.5, 11.0],
            "wind_speed_10m": [12.0, 14.0, 13.0],
            "wind_gusts_10m": [18.0, 20.0, 19.5],
            "wind_direction_10m": [180, 185, 190],
            "surface_pressure": [1010.0, 1010.5, 1011.0],
            "pressure_msl": [1013.0, 1013.5, 1014.0],
            "rain": [0.0, 0.0, 0.1],
            "shortwave_radiation": [400, 450, 500],
            "cloud_cover": [10, 15, 20],
            "cloud_cover_low": [5, 10, 15],
            "cloud_cover_mid": [3, 4, 5],
            "cloud_cover_high": [2, 1, 0],
            "precipitation": [0.0, 0.0, 0.1],
            "weather_code": [1, 2, 3],
            "is_day": [1, 1, 1],
            "sunshine_duration": [3600, 3600, 3600],
            "direct_radiation": [300, 350, 400],
            "diffuse_radiation": [100, 100, 100],
            "et0_fao_evapotranspiration": [0.2, 0.3, 0.3],
            "snowfall": [0.0, 0.0, 0.0],
        },
        "daily": {
            "time": ["2025-10-09"],
            "sunrise": ["2025-10-09T06:30"],
            "sunset": ["2025-10-09T18:00"],
            "daylight_duration": [41400],
            "sunshine_duration": [40000],
            "temperature_2m_max": [25.0],
            "temperature_2m_min": [15.0],
            "precipitation_sum": [0.1],
        },
    }


def test_hourly_variables_have_a_rename_for_each() -> None:
    for var in HOURLY_VARIABLES:
        assert var in HOURLY_RENAME, f"{var} missing from HOURLY_RENAME"


def test_daily_variables_have_a_rename_for_each() -> None:
    for var in DAILY_VARIABLES:
        assert var in DAILY_RENAME, f"{var} missing from DAILY_RENAME"


def test_renames_produce_canonical_base_prefix() -> None:
    for canonical in HOURLY_RENAME.values():
        assert canonical.startswith("base_")
    for canonical in DAILY_RENAME.values():
        assert canonical.startswith("base_")


def test_build_hourly_dataframe_full_payload() -> None:
    station = make_test_station()
    payload = make_full_payload()
    df = _build_hourly_dataframe(station, payload)

    assert len(df) == 3
    assert df["station_id"].iloc[0] == "ITEST_FAKE"
    assert "valid_time_utc" in df.columns
    assert "base_temperature_c" in df.columns
    assert "base_evapotranspiration_mm" in df.columns
    assert df["base_temperature_c"].iloc[0] == 20.1
    assert df["valid_time_utc"].dt.tz is not None


def test_build_hourly_dataframe_partial_payload_keeps_what_exists() -> None:
    station = make_test_station()
    partial = {
        "hourly": {
            "time": ["2025-10-09T12:00"],
            "temperature_2m": [20.1],
            "relative_humidity_2m": [55],
        }
    }
    df = _build_hourly_dataframe(station, partial)

    assert len(df) == 1
    assert "base_temperature_c" in df.columns
    assert "base_relative_humidity_pct" in df.columns
    assert "base_uv_index" not in df.columns
    assert "base_dew_point_c" not in df.columns


def test_build_hourly_dataframe_empty_payload_returns_empty() -> None:
    station = make_test_station()
    assert _build_hourly_dataframe(station, {}).empty
    assert _build_hourly_dataframe(station, {"hourly": {}}).empty


def test_build_daily_dataframe_full_payload() -> None:
    station = make_test_station()
    payload = make_full_payload()
    df = _build_daily_dataframe(station, payload)

    assert len(df) == 1
    assert "date_utc" in df.columns
    assert "base_sunrise_utc" in df.columns
    assert "base_sunset_utc" in df.columns
    assert df["base_sunrise_utc"].dt.tz is not None
    assert df["base_sunset_utc"].dt.tz is not None


def test_build_daily_dataframe_empty_payload_returns_empty() -> None:
    station = make_test_station()
    assert _build_daily_dataframe(station, {}).empty


def test_fetch_station_baseline_writes_parquet_and_returns_dataframes(
    tmp_path: Path,
) -> None:
    station = make_test_station()
    payload = make_full_payload()

    with patch("postprocessing.ingestion.openmeteo._api_call", return_value=payload), \
         patch(
             "postprocessing.ingestion.openmeteo._hourly_path",
             return_value=tmp_path / "ITEST_FAKE_hourly.parquet",
         ), \
         patch(
             "postprocessing.ingestion.openmeteo._daily_path",
             return_value=tmp_path / "ITEST_FAKE_daily.parquet",
         ):
        hourly_df, daily_df = fetch_station_baseline(station)

    assert len(hourly_df) == 3
    assert len(daily_df) == 1
    assert (tmp_path / "ITEST_FAKE_hourly.parquet").is_file()
    assert (tmp_path / "ITEST_FAKE_daily.parquet").is_file()

    reloaded = pd.read_parquet(tmp_path / "ITEST_FAKE_hourly.parquet")
    assert "base_temperature_c" in reloaded.columns
    assert reloaded["base_temperature_c"].iloc[0] == 20.1


def test_fetch_station_baseline_uses_cache_when_complete(tmp_path: Path) -> None:
    station = make_test_station()
    payload = make_full_payload()
    start = datetime(2025, 10, 9, 12, tzinfo=timezone.utc)
    end = datetime(2025, 10, 9, 14, tzinfo=timezone.utc)

    with patch("postprocessing.ingestion.openmeteo._api_call", return_value=payload) as mock_api, \
         patch(
             "postprocessing.ingestion.openmeteo._hourly_path",
             return_value=tmp_path / "ITEST_FAKE_hourly.parquet",
         ), \
         patch(
             "postprocessing.ingestion.openmeteo._daily_path",
             return_value=tmp_path / "ITEST_FAKE_daily.parquet",
         ):
        fetch_station_baseline(station, start=start, end=end)
        assert mock_api.call_count == 1

        fetch_station_baseline(station, start=start, end=end)
        assert mock_api.call_count == 1


def test_fetch_station_baseline_force_refresh_skips_cache(tmp_path: Path) -> None:
    station = make_test_station()
    payload = make_full_payload()

    with patch("postprocessing.ingestion.openmeteo._api_call", return_value=payload) as mock_api, \
         patch(
             "postprocessing.ingestion.openmeteo._hourly_path",
             return_value=tmp_path / "ITEST_FAKE_hourly.parquet",
         ), \
         patch(
             "postprocessing.ingestion.openmeteo._daily_path",
             return_value=tmp_path / "ITEST_FAKE_daily.parquet",
         ):
        fetch_station_baseline(station)
        fetch_station_baseline(station, force_refresh=True)
        assert mock_api.call_count == 2


def test_has_complete_cache_returns_false_for_missing_files(tmp_path: Path) -> None:
    start = datetime(2025, 10, 9, tzinfo=timezone.utc)
    end = datetime(2025, 10, 11, tzinfo=timezone.utc)
    assert not _has_complete_cache(
        tmp_path / "missing_hourly.parquet",
        tmp_path / "missing_daily.parquet",
        start,
        end,
    )


def test_open_meteo_fetch_error_can_be_raised() -> None:
    with pytest.raises(OpenMeteoFetchError):
        raise OpenMeteoFetchError("test")
