from __future__ import annotations

import pandas as pd

DEFAULT_LEADS_HOURS: tuple[int, ...] = tuple(range(1, 73))


def add_leaded_targets(
    df: pd.DataFrame,
    target_columns: list[str],
    leads: tuple[int, ...] = DEFAULT_LEADS_HOURS,
) -> pd.DataFrame:
    out = df.copy()
    if "station_id" not in out.columns or "valid_time_utc" not in out.columns:
        raise KeyError(
            "add_leaded_targets requires 'station_id' and 'valid_time_utc' columns."
        )
    out = out.sort_values(["station_id", "valid_time_utc"]).reset_index(drop=True)

    for target in target_columns:
        if target not in out.columns:
            continue
        for lead in leads:
            new_col = f"{target}_lead_{lead}h"
            out[new_col] = out.groupby("station_id")[target].shift(-lead)
    return out


def build_horizon_table(
    df: pd.DataFrame,
    target_columns: list[str],
    leads: tuple[int, ...] = DEFAULT_LEADS_HOURS,
    drop_incomplete: bool = True,
) -> pd.DataFrame:
    out = add_leaded_targets(df, target_columns, leads)

    out = out.rename(columns={"valid_time_utc": "issue_time_utc"})

    if drop_incomplete:
        leaded_cols = [
            f"{t}_lead_{lead}h"
            for t in target_columns
            if t in out.columns
            for lead in leads
        ]
        if leaded_cols:
            mask_any_leaded_present = out[leaded_cols].notna().any(axis=1)
            out = out[mask_any_leaded_present].reset_index(drop=True)

    return out
