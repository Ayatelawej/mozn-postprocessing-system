from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import numpy as np
import pandas as pd

from postprocessing.horizon.framing import build_horizon_table
from postprocessing.qc.gates import gate_column_for, load_overrides
from postprocessing.utils.paths import get_paths


TARGET_TO_TARGET_COLUMNS: dict[str, tuple[str, ...]] = {
    "temperature": ("temperature_residual_c",),
    "relative_humidity": ("relative_humidity_residual_pct",),
    "dew_point": ("dew_point_residual_c",),
    "wind_speed": ("wind_speed_residual_kmh",),
    "wind_gust": ("wind_gust_residual_kmh",),
    "pressure": ("pressure_residual_max_hpa",),
    "wind_direction": ("winddir_residual_sin", "winddir_residual_cos"),
    "rain_occurrence": ("rain_occurrence",),
    "rain_amount": ("rain_amount_log1p",),
    "uv": ("uv_index",),
}


PREDICTS_RESIDUAL: frozenset[str] = frozenset({
    "temperature", "relative_humidity", "dew_point",
    "wind_speed", "wind_gust", "pressure",
})


VALIDITY_TIME_LEAD_COLUMNS: tuple[str, ...] = (
    "base_temperature_c",
    "base_relative_humidity_pct",
    "base_dew_point_c",
    "base_wind_speed_kmh",
    "base_wind_gust_kmh",
    "base_wind_direction_deg",
    "base_wind_direction_sin",
    "base_wind_direction_cos",
    "base_wind_u_kmh",
    "base_wind_v_kmh",
    "base_msl_pressure_hpa",
    "base_cloud_cover_pct",
    "base_cloud_cover_low_pct",
    "base_cloud_cover_mid_pct",
    "base_cloud_cover_high_pct",
    "base_is_day",
    "base_precipitation_mm",
    "base_rain_total_mm",
    "base_rain_rolling_3h_mm",
    "base_rain_rolling_6h_mm",
    "base_solar_radiation_wm2",
    "base_direct_radiation_wm2",
    "base_diffuse_radiation_wm2",
    "base_sunshine_seconds",
    "hour_sin",
    "hour_cos",
    "is_day_flag",
    "minutes_from_sunrise",
    "solar_progress_0_1",
    "solar_centered",
    "clipped_shortwave_wm2",
    "cloud_attenuation_factor",
    "sunshine_fraction",
    "uv_proxy",
    "solar_to_clear_sky_ratio",
)


def known_targets() -> tuple[str, ...]:
    return tuple(TARGET_TO_TARGET_COLUMNS.keys())


def target_columns_for(target: str) -> tuple[str, ...]:
    if target not in TARGET_TO_TARGET_COLUMNS:
        raise KeyError(
            f"Unknown target '{target}'. Known: {known_targets()}"
        )
    return TARGET_TO_TARGET_COLUMNS[target]


def predicts_residual(target: str) -> bool:
    return target in PREDICTS_RESIDUAL


def default_canonical_path() -> Path:
    return get_paths().data.processed_dir / "canonical_hourly_v1.parquet"


def load_canonical(path: Path | str | None = None) -> pd.DataFrame:
    if path is None:
        path = default_canonical_path()
    df = pd.read_parquet(path)
    if not pd.api.types.is_datetime64_any_dtype(df["valid_time_utc"]):
        df["valid_time_utc"] = pd.to_datetime(df["valid_time_utc"], utc=True)
    elif df["valid_time_utc"].dt.tz is None:
        df["valid_time_utc"] = df["valid_time_utc"].dt.tz_localize("UTC")
    return df


def prepare_target_frame(
    df: pd.DataFrame,
    target: str,
    leads: tuple[int, ...],
    *,
    apply_overrides: bool = True,
) -> pd.DataFrame:
    if not leads:
        raise ValueError("leads must be a non-empty tuple")
    target_cols = target_columns_for(target)
    gate_col = gate_column_for(target)

    columns_to_lead: list[str] = list(target_cols)
    if gate_col not in columns_to_lead:
        columns_to_lead.append(gate_col)
    for col in VALIDITY_TIME_LEAD_COLUMNS:
        if col in df.columns and col not in columns_to_lead:
            columns_to_lead.append(col)

    framed = build_horizon_table(df, columns_to_lead, leads, drop_incomplete=False)

    keep = pd.Series(False, index=framed.index)
    for lead in leads:
        gate_lead = f"{gate_col}_lead_{lead}h"
        if gate_lead not in framed.columns:
            continue
        gate_ok = framed[gate_lead].fillna(False).astype(bool)
        target_ok = pd.Series(True, index=framed.index)
        for tc in target_cols:
            target_lead = f"{tc}_lead_{lead}h"
            if target_lead in framed.columns:
                target_ok = target_ok & framed[target_lead].notna()
        keep = keep | (gate_ok & target_ok)

    framed = framed[keep].reset_index(drop=True)

    if apply_overrides:
        excluded = {
            o.get("station_id")
            for o in load_overrides()
            if o.get("status") == "unusable" and o.get("target") == target
        }
        excluded.discard(None)
        if excluded:
            framed = framed[~framed["station_id"].isin(excluded)].reset_index(drop=True)

    return framed


def loso_split(df: pd.DataFrame, holdout_station: str) -> tuple[np.ndarray, np.ndarray]:
    available = set(df["station_id"].unique())
    if holdout_station not in available:
        raise ValueError(
            f"holdout_station '{holdout_station}' not in DataFrame; "
            f"available: {sorted(available)}"
        )
    val_mask = (df["station_id"] == holdout_station).to_numpy()
    val_idx = np.flatnonzero(val_mask)
    train_idx = np.flatnonzero(~val_mask)
    return train_idx, val_idx


def within_station_hourly_split(
    df: pd.DataFrame,
    fraction: float = 0.10,
    block_hours: int = 48,
    *,
    seed: int = 42,
    time_col: str = "issue_time_utc",
) -> tuple[np.ndarray, np.ndarray]:
    if not 0.0 < fraction < 1.0:
        raise ValueError("fraction must be in (0, 1)")
    if block_hours <= 0:
        raise ValueError("block_hours must be positive")
    if time_col not in df.columns:
        raise KeyError(f"time_col '{time_col}' not in DataFrame")
    rng = np.random.default_rng(seed)
    val_mask = np.zeros(len(df), dtype=bool)
    ordered = df.sort_values(["station_id", time_col])
    for station, group in ordered.groupby("station_id", sort=False):
        positions = group.index.to_numpy()
        n = len(positions)
        if n == 0:
            continue
        n_blocks = max(1, n // block_hours)
        target_val_rows = int(round(n * fraction))
        n_val_blocks = max(1, target_val_rows // block_hours)
        n_val_blocks = min(n_val_blocks, n_blocks)
        chosen = rng.choice(n_blocks, size=n_val_blocks, replace=False)
        for b in chosen:
            start = int(b) * block_hours
            end = min(start + block_hours, n)
            val_mask[positions[start:end]] = True
    val_idx = np.flatnonzero(val_mask)
    train_idx = np.flatnonzero(~val_mask)
    return train_idx, val_idx


def walk_forward_splits(
    df: pd.DataFrame,
    n_folds: int = 5,
    *,
    time_col: str = "issue_time_utc",
) -> Iterator[tuple[np.ndarray, np.ndarray]]:
    if n_folds < 2:
        raise ValueError("n_folds must be at least 2")
    if time_col not in df.columns:
        raise KeyError(f"time_col '{time_col}' not in DataFrame")
    times = pd.to_datetime(df[time_col], utc=True)
    t_min, t_max = times.min(), times.max()
    boundaries = pd.date_range(t_min, t_max, periods=n_folds + 1)
    times_np = times.to_numpy()
    for k in range(1, n_folds):
        train_end = boundaries[k]
        val_end = boundaries[k + 1]
        train_mask = times_np < train_end
        val_mask = (times_np >= train_end) & (times_np < val_end)
        train_idx = np.flatnonzero(train_mask)
        val_idx = np.flatnonzero(val_mask)
        if len(train_idx) == 0 or len(val_idx) == 0:
            continue
        yield train_idx, val_idx
