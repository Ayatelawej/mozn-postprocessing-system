# Deployment - Mozn Corrected-Forecasts Service

Two processes on the same server as the Mozn backend:

- **pipeline** - runs hourly, writes `latest.json` by fetching observations and Open-Meteo data, predicting, gating, and writing output.
- **api** - always-on, serves `latest.json` at `http://127.0.0.1:${FORECAST_API_PORT}/forecasts` on localhost only.

## Fill These Blanks First

- Install path: these files assume `/opt/mozn-postprocessing`
- Venv path: `/opt/mozn-postprocessing/.venv`
- Service user: `mozn`
- Confirm the server is Linux with systemd

## One-Time Setup

1. Put the repo at the install path and create the venv:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements/prod.txt
.venv/bin/pip install -e .
```

2. Create the secrets file, root-owned and not world-readable:

```bash
sudo cp deploy/mozn-forecasts.env.example /etc/mozn-forecasts.env
sudo nano /etc/mozn-forecasts.env
sudo chown root:mozn /etc/mozn-forecasts.env
sudo chmod 640 /etc/mozn-forecasts.env
```

Use:

- `AI_API_KEY`: token used to read the backend observations API
- `FORECAST_API_TOKEN`: a new strong secret for the backend to call this forecasts API
- `BACKEND_BASE_URL`: internal backend base URL; `/api/ai/observations` is appended automatically
- `FORECAST_API_PORT`: localhost port for this forecasts API

Generate `FORECAST_API_TOKEN` with:

```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

3. Install the units:

```bash
sudo cp deploy/mozn-forecast-*.service deploy/mozn-forecast-*.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

4. Enable and start:

```bash
sudo systemctl enable --now mozn-forecast-api.service
sudo systemctl enable --now mozn-forecast-pipeline.timer
```

5. Kick one pipeline run now:

```bash
sudo systemctl start mozn-forecast-pipeline.service
```

## Verify

```bash
systemctl status mozn-forecast-api.service
systemctl list-timers mozn-forecast-pipeline.timer
journalctl -u mozn-forecast-pipeline.service -n 50
curl http://127.0.0.1:8000/health
curl -H "Authorization: Bearer <FORECAST_API_TOKEN>" http://127.0.0.1:8000/forecasts
```

Replace `8000` with `FORECAST_API_PORT` if you set a different port.

Expected `/health` response after the first pipeline run:

```json
{"status":"ok","has_forecast":true}
```

## Updating After A Code Change

```bash
cd /opt/mozn-postprocessing
git pull
.venv/bin/pip install -r requirements/prod.txt
sudo systemctl restart mozn-forecast-api.service
```

The pipeline picks up changes on its next timer run.

## Cron Alternative

Use this only if cron is preferred over a systemd timer:

```cron
5 * * * * cd /opt/mozn-postprocessing && set -a && . /etc/mozn-forecasts.env && set +a && .venv/bin/python scripts/inference/run_pipeline.py >> /var/log/mozn-forecast.log 2>&1
```
