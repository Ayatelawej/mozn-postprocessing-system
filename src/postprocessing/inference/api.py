from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query

app = FastAPI(title="Mozn corrected-forecasts API")
_cache = {}


def _latest_path():
    return Path(os.environ.get("FORECAST_DIR", "outputs/forecasts")) / "latest.json"


def _load():
    p = _latest_path()
    if not p.exists():
        raise HTTPException(status_code=503, detail="no forecast available yet")
    mtime = p.stat().st_mtime
    key = str(p)
    if _cache.get(key, {}).get("mtime") != mtime:
        _cache[key] = {"mtime": mtime, "data": json.loads(p.read_text(encoding="utf-8"))}
    return _cache[key]["data"]


def _auth(authorization):
    token = os.environ.get("FORECAST_API_TOKEN", "")
    if token and authorization != f"Bearer {token}":
        raise HTTPException(status_code=401, detail="invalid or missing token")


@app.get("/health")
def health():
    return {"status": "ok", "has_forecast": _latest_path().exists()}


@app.get("/forecasts")
def forecasts(authorization: str = Header(default=""), station: Optional[str] = Query(default=None)):
    _auth(authorization)
    data = _load()
    if station is None:
        return data
    sub = [s for s in data["stations"] if s["station_id"] == station or s["wu_id"] == station]
    if not sub:
        raise HTTPException(status_code=404, detail=f"station '{station}' not found")
    out = {k: v for k, v in data.items() if k != "stations"}
    out["n_stations"] = len(sub)
    out["stations"] = sub
    return out
