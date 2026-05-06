from __future__ import annotations

import pandas as pd

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


def is_trainable(df: pd.DataFrame, target: str) -> pd.Series:
    col = gate_column_for(target)
    if col not in df.columns:
        raise KeyError(
            f"DataFrame is missing gate column '{col}' for target '{target}'. "
            "Run validate_station_table.py before applying QC gates."
        )
    return df[col].fillna(False).astype(bool)


def filter_to_trainable(df: pd.DataFrame, target: str) -> pd.DataFrame:
    mask = is_trainable(df, target)
    return df[mask].reset_index(drop=True)


def trainable_rows_per_target(df: pd.DataFrame) -> dict[str, int]:
    counts: dict[str, int] = {}
    for target, col in TRAINABILITY_GATE_COLUMNS.items():
        if col in df.columns:
            counts[target] = int(df[col].fillna(False).astype(bool).sum())
    return counts
