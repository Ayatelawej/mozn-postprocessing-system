# Mozn Post-Processing System

Open-source weather forecast post-processing system for correcting baseline forecasts using pooled station observations.

## Current status

The repository contains the data foundation, model training pipeline, production inference pipeline, validation diagnostics, and deployment assets for the post-processing system.

Completed so far:

- project scaffold and environment setup
- data contract and data-source documentation
- pooled hourly station dataset and station registry
- raw CSV inspection, profiling, validation, and partitioned parquet conversion
- Open-Meteo baseline ingestion and canonical training-frame generation
- target-specific feature engineering and validation gates
- LOSO, within-station, and April out-of-sample model validation
- full 72-lead artifact training for the eight shipped targets
- live inference assembly, reconstruction, confidence gating, and JSON output
- FastAPI forecast endpoint and systemd/Docker deployment assets

## Project goal

Build a reproducible, portable, open-source post-processing pipeline that:

- ingests pooled station observations
- pulls baseline forecasts through project code
- standardizes and validates the data
- applies quality-control and station-health logic
- builds canonical training and inference tables
- trains target-specific post-processing models
- supports inference, backend integration, and deployment

## Target groups

### Shipped in v1

- temperature
- relative_humidity
- dew_point
- wind_speed
- wind_gust
- pressure
- uv
- wind_direction

### Investigated but not shipped in v1

- rain_occurrence
- rain_amount

## Repository structure

- `configs/`
  - project configuration, schema, target definitions, mappings, validation rules

- `data/`
  - raw data, metadata, manifests, samples, parquet outputs

- `docs/`
  - data contract, data-source notes, manuals, phase documentation, working rules

- `scripts/`
  - reproducible pipeline scripts for inspection, conversion, standardization, validation, training, and inference

- `src/`
  - reusable project package code

- `reports/`
  - diagnostics and generated summaries

- `tests/`
  - unit, integration, and smoke tests

- `deploy/`
  - systemd and Docker deployment assets

- `Dockerfile`, `docker-compose.yml`
  - container deployment entry points for the forecast API and scheduled pipeline

## Data policy

- raw files are treated as immutable once landed
- generated outputs must be reproducible from code
- bulky artifacts may be committed when they are deliberate versioned dataset layers
- interim and frequently regenerated working artifacts should remain untracked
- all important source files should be reflected in manifests and documentation

## Current data foundation

Primary raw station input:

- `data/raw/stations/station_hourly_merged_countrywide_v1.csv`

Current preprocessing and validation outputs:

- raw station profile JSON
- raw station column manifest CSV
- partitioned parquet station dataset
- standardized station parquet build artifact
- station validation summary JSON
- station-level validation summary CSV
- target-level validation summary CSV
- variable-level validation summary CSV

## Manuals and supporting references

Weather-station manuals and supporting setup references are stored under:

- `docs/manuals/`

These documents support interpretation of installation-sensitive variables such as wind direction, wind speed, gust, pressure, humidity drift, rainfall behavior, and sunlight-related measurements.

## Inference and deployment

Production inference writes corrected forecasts to:

- `outputs/forecasts/latest.json`

The forecast API serves that file at:

- `GET /health`
- `GET /forecasts`
- `GET /forecasts?station=<station_id_or_wu_id>`

Deployment options are documented in:

- `deploy/README.md`
- `deploy/DOCKER.md`

Generated forecast output and local secrets are ignored by Git.

## Reproducibility

Use the requirements files depending on purpose:

- `requirements/base.txt`
- `requirements/dev.txt`
- `requirements/prod.txt`
- `requirements/research.txt`

Environment variables are documented in `.env.example`.

## Open-source note

This repository is intended to remain open, reproducible, and understandable.
