# Data Layer

This directory contains the data foundation for the Mozn AI post-processing system.

## Structure

- `raw/stations/`
  - Original station observation files
  - Raw files should remain unchanged after placement

- `raw/metadata/`
  - Station metadata
  - Manuals
  - Station lists and supporting documentation

- `external/openmeteo/`
  - Baseline forecast pulls used as the model reference source

- `external/reference/`
  - Helper files such as mappings, lookup tables, and public reference material

- `interim/stations_clean/`
  - Cleaned but not fully merged station tables

- `interim/aligned/`
  - Timestamp-aligned intermediate tables

- `processed/train_tables/`
  - Canonical training tables

- `processed/inference_tables/`
  - Canonical inference tables

- `manifests/`
  - File inventories and data manifests

## Rules

- Raw files are immutable once placed
- Generated outputs are reproducible from code
- Large open datasets (may) be distributed outside the git repository if they are documented and publicly accessible
- The repository documents how to rebuild all processed tables from raw and external inputs

## Key principle

The final system will be portable, open, and reproducible.
