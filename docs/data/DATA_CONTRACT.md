# Data Contract

## Purpose

This document defines the minimum requirements for raw station data, baseline forecast data, and canonical merged tables used by the Mozn AI post-processing system.

## Required identifiers

Every station observation table must support:

- `station_id`
- `timestamp_utc`

Every baseline forecast table must support:

- `station_id`
- `issue_time_utc`
- `valid_time_utc`
- `lead_h`

## Station data principles

- Raw station data is immutable after placement in `data/raw/stations/`
- Timestamp values must be normalized to UTC
- Units must be standardized before entry into canonical processed tables
- Pressure fields must remain clearly separated until the pressure decision layer is implemented
- Rain total and rain rate must remain separate fields
- Wind direction must remain in degrees until circular handling is implemented

## Baseline forecast principles

- Baseline forecast data is stored separately from station truth
- Baseline timestamps must preserve issue time and valid time
- Forecast lead must be explicit, not inferred later
- Source identity must be preserved where available

## Canonical table principles

- Canonical train and inference tables must be reproducible from raw station data and external forecast data
- Canonical tables must include QC and station-health-ready placeholders
- No model training should depend on notebook-only transformations

## Open-source principles

- Data access must be documented clearly
- If bulky data is not committed directly, its public access path must be documented
- Any user must be able to understand how raw files become processed tables
