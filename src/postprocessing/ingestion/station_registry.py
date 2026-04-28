from __future__ import annotations

from datetime import datetime
from functools import lru_cache
from pathlib import Path

import pandas as pd
from pydantic import BaseModel

from postprocessing.utils.paths import get_paths


class Station(BaseModel):
    station_id: str
    station_name: str
    city: str
    country: str
    latitude: float
    longitude: float
    elevation_m: float
    install_date: datetime
    pooled_first_timestamp: datetime
    pooled_last_timestamp: datetime
    pooled_rows: int
    pooled_present_hours: int

    @property
    def fetch_start(self) -> datetime:
        return self.pooled_first_timestamp

    @property
    def fetch_end(self) -> datetime:
        return self.pooled_last_timestamp


def _registry_path() -> Path:
    return get_paths().data.manifests_dir / "station_registry.csv"


@lru_cache(maxsize=1)
def load_stations() -> list[Station]:
    df = pd.read_csv(_registry_path())

    for col in ["install_date", "pooled_first_timestamp", "pooled_last_timestamp"]:
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    records = df.to_dict(orient="records")
    return [Station(**{k: v for k, v in r.items() if pd.notna(v)}) for r in records]


def get_station(station_id: str) -> Station:
    stations = load_stations()
    for s in stations:
        if s.station_id == station_id:
            return s
    raise KeyError(f"Station not found in registry: {station_id}")
