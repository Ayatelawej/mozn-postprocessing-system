# Mozn Post-Processing System

Open-source weather forecast post-processing system for correcting baseline forecasts using pooled station observations.

## Current status

The repository currently contains the data foundation and early preprocessing pipeline for the post-processing system.

Completed so far:

- project scaffold and environment setup
- data contract and data-source documentation
- landed pooled hourly station dataset
- station registry and raw file manifest
- raw CSV inspection and profiling
- raw CSV to partitioned parquet conversion
- first-pass standardized station table generation

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

### Core

- temperature
- relative_humidity
- dew_point
- wind_speed
- uv

### Experimental

- pressure
- wind_gust

### Specialized

- rain_occurrence
- rain_amount
- wind_direction

### Derived

- wind_chill
- heat_index

## Repository structure

- `configs/`
  - project configuration, schema, target definitions, mappings

- `data/`
  - raw data, metadata, manifests, samples, parquet outputs

- `docs/`
  - data contract, data-source notes, phase documentation

- `scripts/`
  - reproducible pipeline scripts for inspection, conversion, standardization, training, and inference

- `src/`
  - reusable project package code

- `reports/`
  - diagnostics and generated summaries

- `tests/`
  - unit, integration, and smoke tests

- `deployment/`
  - deployment-related assets

## Data policy

- raw files are treated as immutable once landed
- generated outputs must be reproducible from code
- bulky artifacts may be committed when they are deliberate versioned dataset layers
- interim and frequently regenerated working artifacts should remain untracked
- all important source files should be reflected in manifests and documentation

## Current data foundation

Primary raw station input:

- `data/raw/stations/station_hourly_merged_countrywide_v1.csv`

Current preprocessing outputs:

- raw station profile JSON
- raw station column manifest CSV
- partitioned parquet station dataset
- standardized station parquet build artifact

## Next steps

- validate and gate the standardized data layer
- add QC-focused checks and missingness rules
- normalize timestamp handling and schema enforcement
- design baseline forecast ingestion
- build canonical train and inference tables
- begin target-specific modeling and evaluation

## Reproducibility

Use the requirements files depending on purpose:

- `requirements/base.txt`
- `requirements/dev.txt`
- `requirements/prod.txt`
- `requirements/research.txt`

Environment variables are documented in `.env.example`.

## Open-source note

This repository is intended to remain open, reproducible, and understandable.
