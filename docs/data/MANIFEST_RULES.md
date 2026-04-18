# Manifest Rules

Each major file added to the data layer should be recorded in `data/manifests/raw_files.csv`.

## Required fields

- `file_name`
- `file_role`
- `source_type`
- `source_name`
- `station_id`
- `time_coverage_start`
- `time_coverage_end`
- `timezone`
- `row_count`
- `status`
- `notes`

## File role examples

- `raw_station_observations`
- `baseline_forecast_pull`
- `station_metadata`
- `public_reference`
- `sample_data`

## Status examples

- `active`
- `archived`
- `sample`
- `draft`

## Rule

No important data file should appear in the project without being reflected in the manifest.
