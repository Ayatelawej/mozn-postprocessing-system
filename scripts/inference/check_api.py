from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

tmp = tempfile.mkdtemp()
os.environ["FORECAST_DIR"] = tmp
os.environ["FORECAST_API_TOKEN"] = "secret123"

from fastapi.testclient import TestClient
from postprocessing.inference.api import app

client = TestClient(app)
ok = True


def check(name, cond):
    global ok
    ok = ok and cond
    print(f"  {'OK' if cond else 'BAD'}  {name}")


r = client.get("/health")
check("health (no file) 200 + has_forecast False", r.status_code == 200 and r.json()["has_forecast"] is False)
r = client.get("/forecasts", headers={"Authorization": "Bearer secret123"})
check("forecasts (no file) -> 503", r.status_code == 503)

payload = {"issue_time_utc": "2026-06-13T11:00:00Z", "generated_at_utc": "2026-06-13T11:30:02Z", "n_stations": 2, "stations": [
    {"station_id": "u1", "wu_id": "ITRIPO33", "recent_obs_frac": 1.0, "targets": {"temperature": [{"lead": 1, "valid_time_utc": "2026-06-13T12:00:00Z", "value": 33.0, "status": "ok", "reason": ""}]}},
    {"station_id": "u2", "wu_id": "IJANZO4", "recent_obs_frac": 0.0, "targets": {"pressure": [{"lead": 1, "valid_time_utc": "2026-06-13T12:00:00Z", "value": 1011.0, "status": "fallback", "reason": "insufficient_recent_obs"}]}}]}
Path(tmp, "latest.json").write_text(json.dumps(payload))

r = client.get("/forecasts")
check("no token -> 401", r.status_code == 401)
r = client.get("/forecasts", headers={"Authorization": "Bearer wrong"})
check("wrong token -> 401", r.status_code == 401)
r = client.get("/forecasts", headers={"Authorization": "Bearer secret123"})
check("valid token -> 200, n=2", r.status_code == 200 and r.json()["n_stations"] == 2)
r = client.get("/forecasts?station=ITRIPO33", headers={"Authorization": "Bearer secret123"})
j = r.json()
check("filter by wu_id -> 1 station", r.status_code == 200 and j["n_stations"] == 1 and j["stations"][0]["wu_id"] == "ITRIPO33")
r = client.get("/forecasts?station=u2", headers={"Authorization": "Bearer secret123"})
check("filter by station_id", r.status_code == 200 and r.json()["stations"][0]["station_id"] == "u2")
r = client.get("/forecasts?station=NOPE", headers={"Authorization": "Bearer secret123"})
check("unknown station -> 404", r.status_code == 404)
r = client.get("/health")
check("health (with file) has_forecast True", r.json()["has_forecast"] is True)

print("\nAPI CHECK", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
