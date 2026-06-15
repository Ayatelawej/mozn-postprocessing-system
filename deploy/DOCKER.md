# Deployment With Docker

Two containers are built from one image and share a volume for the forecast file:

- **forecast-pipeline** - runs the pipeline once an hour and writes `latest.json` to the shared volume.
- **forecast-api** - serves `GET /forecasts` from that volume, published on `127.0.0.1:${FORECAST_API_PORT:-8000}`.

## Setup

1. Create the env file from the template and fill in the tokens and service addresses:

```bash
cp .env.example .env
```

Edit `.env`:

- `AI_API_KEY`: token for the backend observations API
- `FORECAST_API_TOKEN`: a strong secret the backend uses to call this API
- `BACKEND_BASE_URL`: internal backend base URL; `/api/ai/observations` is appended automatically
- `FORECAST_API_PORT`: local port for this forecasts API

This file is gitignored. Do not commit it.

2. Build and start:

```bash
docker compose up -d --build
```

3. Check it:

```bash
docker compose ps
docker compose logs -f forecast-pipeline
curl http://127.0.0.1:8000/health
curl -H "Authorization: Bearer <FORECAST_API_TOKEN>" http://127.0.0.1:8000/forecasts
```

Replace `8000` with `FORECAST_API_PORT` if you set a different port.

`/health` returns `has_forecast: true` after the first successful pipeline run.

## How The Backend Reaches The API

Backend on the host, using the default port:

```text
http://127.0.0.1:8000/forecasts
```

Replace `8000` with `FORECAST_API_PORT` if you set a different port.

Backend in Docker:

```text
http://forecast-api:8000/forecasts
```

If the backend is in Docker, put it on the same compose project/network and call the API by service name. The host port mapping is then optional.

## Updating After A Code Change

```bash
git pull
docker compose up -d --build
```

## Notes

- The pipeline runs `scripts/inference/run_scheduler.py`: once on start, then hourly at `:05`.
- Forecasts persist in the named volume `forecast-data`; restarts do not lose them.
- The API binds `0.0.0.0` inside the container but is published only on `127.0.0.1`, so it is not exposed to the public internet.
- The systemd unit files in `deploy/` remain as a non-Docker alternative.
