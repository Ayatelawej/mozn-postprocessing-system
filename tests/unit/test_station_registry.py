from datetime import datetime

import pytest

from postprocessing.ingestion.station_registry import (
    Station,
    get_station,
    load_stations,
)


def test_load_stations_returns_at_least_baseline_count() -> None:
    stations = load_stations()
    assert len(stations) >= 26


def test_every_record_is_a_station() -> None:
    stations = load_stations()
    for s in stations:
        assert isinstance(s, Station)


def test_station_ids_are_unique() -> None:
    stations = load_stations()
    ids = [s.station_id for s in stations]
    assert len(set(ids)) == len(ids)


def test_known_station_in_registry() -> None:
    s = get_station("INUQAT8")
    assert s.city == "Ragdalin"
    assert s.country == "Libya"
    assert s.elevation_m == 4


def test_known_low_activity_station_present() -> None:
    s = get_station("I90583612")
    assert s.pooled_present_hours == 605
    assert s.pooled_rows == 3726


def test_unknown_station_raises_key_error() -> None:
    with pytest.raises(KeyError):
        get_station("DOES_NOT_EXIST")


def test_timestamps_are_timezone_aware() -> None:
    stations = load_stations()
    for s in stations:
        assert s.pooled_first_timestamp.tzinfo is not None
        assert s.pooled_last_timestamp.tzinfo is not None


def test_fetch_window_makes_sense() -> None:
    stations = load_stations()
    for s in stations:
        assert s.fetch_start < s.fetch_end
        assert s.fetch_end <= datetime.fromisoformat("2026-04-01T00:00:00+00:00")


def test_all_stations_in_libya() -> None:
    stations = load_stations()
    for s in stations:
        assert s.country == "Libya"
        assert 19 < s.longitude < 26 or 9 < s.longitude < 17
        assert 24 < s.latitude < 34


def test_caching_returns_same_list_instance() -> None:
    a = load_stations()
    b = load_stations()
    assert a is b
