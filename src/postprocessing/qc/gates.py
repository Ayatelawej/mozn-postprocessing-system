from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from postprocessing.utils.paths import get_paths

TRAINABILITY_GATE_COLUMNS: dict[str, str] = {
    "temperature": "gate_temperature_ready",
    "relative_humidity": "gate_relative_humidity_ready",
    "dew_point": "gate_dew_point_ready",
    "wind_speed": "gate_wind_speed_ready",
    "uv": "gate_uv_ready",
    "pressure": "gate_pressure_ready",
    "wind_gust": "gate_wind_gust_ready",
    "wind_direction": "gate_wind_direction_ready",
    "rain_occurrence": "gate_rain_occurrence_ready",
    "rain_amount": "gate_rain_amount_ready",
}


def gate_column_for(target: str) -> str:
    if target not in TRAINABILITY_GATE_COLUMNS:
        raise KeyError(
            f"Unknown target '{target}'. Known targets: {sorted(TRAINABILITY_GATE_COLUMNS)}"
        )
    return TRAINABILITY_GATE_COLUMNS[target]


def load_overrides(path: Path | None = None) -> list[dict]:
    if path is None:
        path = get_paths().configs.data_quality_overrides
    if not path.is_file():
        return []
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not raw:
        return []
    return list(raw.get("overrides", []))


def is_trainable(df: pd.DataFrame, target: str, *, apply_overrides: bool = True) -> pd.Series:
    col = gate_column_for(target)
    if col not in df.columns:
        raise KeyError(
            f"DataFrame is missing gate column '{col}' for target '{target}'. "
            "Run validate_station_table.py before applying QC gates."
        )
    mask = df[col].fillna(False).astype(bool)
    if apply_overrides:
        for override in load_overrides():
            if override.get("status") != "unusable":
                continue
            if override.get("target") != target:
                continue
            sid = override.get("station_id")
            if sid is None:
                continue
            mask = mask & (df["station_id"] != sid)
    return mask


def filter_to_trainable(df: pd.DataFrame, target: str, *, apply_overrides: bool = True) -> pd.DataFrame:
    mask = is_trainable(df, target, apply_overrides=apply_overrides)
    return df[mask].reset_index(drop=True)


def trainable_rows_per_target(df: pd.DataFrame, *, apply_overrides: bool = True) -> dict[str, int]:
    counts: dict[str, int] = {}
    for target in TRAINABILITY_GATE_COLUMNS:
        col = TRAINABILITY_GATE_COLUMNS[target]
        if col in df.columns:
            counts[target] = int(is_trainable(df, target, apply_overrides=apply_overrides).sum())
    return counts
