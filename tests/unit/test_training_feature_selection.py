from __future__ import annotations

import pytest

from postprocessing.training import feature_selection as fs
from postprocessing.training.feature_selection import (
    FEATURE_SPECS,
    audit_feature_lists,
    features_for,
    known_targets,
)


def test_known_targets_match_specs():
    assert set(known_targets()) == set(FEATURE_SPECS.keys())


def test_features_for_temperature_lead_1_includes_expected():
    features = features_for("temperature", 1)
    assert "elevation_m" in features
    assert "station_latitude" in features
    assert "temperature_c_lag_1h" in features
    assert "temperature_c_lag_24h" in features
    assert "temperature_c_roll_3h_mean" in features
    assert "base_temperature_c_lead_1h" in features
    assert "hour_sin_lead_1h" in features


def test_features_for_distinct_leads_produce_distinct_columns():
    a = features_for("temperature", 1)
    b = features_for("temperature", 6)
    assert a != b
    assert "base_temperature_c_lead_1h" in a
    assert "base_temperature_c_lead_1h" not in b
    assert "base_temperature_c_lead_6h" in b
    issue_time_cols = [c for c in a if "_lead_" not in c]
    for col in issue_time_cols:
        assert col in b


def test_features_for_wind_speed_includes_elevation_m_at_all_leads():
    for lead in (1, 12, 72):
        assert "elevation_m" in features_for("wind_speed", lead)


def test_features_for_wind_gust_includes_elevation_m_at_all_leads():
    for lead in (1, 12, 72):
        assert "elevation_m" in features_for("wind_gust", lead)


def test_features_for_pressure_never_includes_pressure_trend():
    for lead in (1, 12, 72):
        features = features_for("pressure", lead)
        assert not any("pressure_trend_hpa" in c for c in features)


def test_no_target_uses_station_id():
    for target in known_targets():
        for lead in (1, 12, 72):
            features = features_for(target, lead)
            assert "station_id" not in features


def test_no_target_uses_pressure_trend():
    for target in known_targets():
        for lead in (1, 12, 72):
            features = features_for(target, lead)
            assert not any("pressure_trend_hpa" in c for c in features)


def test_features_for_unknown_target_raises():
    with pytest.raises(KeyError):
        features_for("not_a_target", 1)


def test_features_for_invalid_lead_raises():
    with pytest.raises(ValueError):
        features_for("temperature", 0)


def test_audit_passes_on_real_specs():
    audit_feature_lists()


def test_audit_catches_missing_elevation_m(monkeypatch):
    fake = dict(FEATURE_SPECS)
    fake["wind_speed"] = {
        "issue_time": ("station_latitude",),
        "validity_time": ("base_wind_speed_kmh",),
    }
    monkeypatch.setattr(fs, "FEATURE_SPECS", fake)
    with pytest.raises(AssertionError, match="elevation_m"):
        audit_feature_lists()


def test_audit_catches_pressure_trend(monkeypatch):
    fake = dict(FEATURE_SPECS)
    fake["pressure"] = {
        "issue_time": ("elevation_m", "pressure_trend_hpa"),
        "validity_time": ("base_msl_pressure_hpa",),
    }
    monkeypatch.setattr(fs, "FEATURE_SPECS", fake)
    with pytest.raises(AssertionError, match="pressure_trend_hpa"):
        audit_feature_lists()


def test_audit_catches_station_id(monkeypatch):
    fake = dict(FEATURE_SPECS)
    fake["temperature"] = {
        "issue_time": ("elevation_m", "station_id"),
        "validity_time": (),
    }
    monkeypatch.setattr(fs, "FEATURE_SPECS", fake)
    with pytest.raises(AssertionError, match="station_id"):
        audit_feature_lists()
