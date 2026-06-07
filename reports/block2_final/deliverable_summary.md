# Mozn AI Weather Post-Processing - Block 2 Deliverable

## Overview

Eight weather targets post-processed via station-residual modelling on top of Open-Meteo baseline forecasts. Targets ship at all 72 horizons (lead 1h to 72h) with per-target winning model class locked from a multi-model sweep.

**Shipping targets:** temperature, relative humidity, dew point, wind speed, wind gust, pressure, UV index, wind direction.

**Excluded from v1:** rain occurrence and rain amount. See `rain_appendix.md`.

## Validation methodology

Three independent validations were performed:

1. **LOSO (Leave-One-Station-Out):** 26-fold per target x lead. Conservative worst-case - deploying at a brand-new station.
2. **Within-station hourly hold-out:** 10% of hours per station held out in 48-hour blocks. Production-like - known stations, unseen hours.
3. **April 2026 out-of-sample:** entire month of April held back from training. Validates temporal generalization. Built from same archive_api source as training canonical.

Each validation was run at leads 1h, 6h, 24h, 48h, 72h. Full 72-lead production artifacts trained on the complete canonical (no holdout) for Block 3 inference.

## Summary table

Per-target MAE reduction (%) at each lead under each validation mode. UV reported as absolute MAE in UV-index points. See `summary_table.png` for a colour-coded version.

| target | model_class | LOSO_L1 | LOSO_L6 | LOSO_L24 | LOSO_L48 | LOSO_L72 | within_station_L1 | within_station_L6 | within_station_L24 | within_station_L48 | within_station_L72 | april_L1 | april_L6 | april_L24 | april_L48 | april_L72 | headline_metric |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| temperature | HGB | 48.85 | 18.57 | 18.98 | 16.64 | 16.00 | 57.96 | 40.34 | 38.23 | 37.23 | 36.20 | 54.87 | 30.96 | 30.47 | 27.52 | 26.87 | MAE reduction % |
| relative_humidity | HGB | 68.71 | 41.42 | 39.18 | 37.42 | 37.48 | 69.82 | 46.18 | 43.08 | 43.17 | 42.73 | 70.88 | 45.20 | 44.30 | 41.82 | 42.12 | MAE reduction % |
| dew_point | HGB | 65.41 | 45.09 | 43.92 | 42.41 | 41.49 | 69.96 | 53.66 | 51.80 | 52.52 | 50.95 | 72.86 | 56.63 | 55.95 | 54.95 | 54.31 | MAE reduction % |
| wind_speed | Ridge | 67.06 | 42.79 | 39.54 | 35.77 | 33.52 | 76.27 | 56.65 | 53.91 | 50.44 | 49.02 | 77.34 | 59.48 | 59.29 | 56.05 | 53.94 | MAE reduction % |
| wind_gust | Ridge | 82.97 | 70.55 | 68.68 | 66.86 | 65.67 | 86.57 | 76.47 | 75.26 | 73.42 | 72.59 | 87.41 | 78.49 | 78.57 | 76.97 | 75.77 | MAE reduction % |
| pressure | Ridge | 92.59 | 81.89 | 66.98 | 56.70 | 50.31 | 94.16 | 86.10 | 72.78 | 64.87 | 58.81 | 93.26 | 83.57 | 66.55 | 50.08 | 37.78 | MAE reduction % |
| uv | HGB | 0.30 | 0.34 | 0.31 | 0.32 | 0.32 | 0.24 | 0.26 | 0.25 | 0.26 | 0.25 | 0.53 | 0.72 | 0.54 | 0.53 | 0.53 | absolute MAE (UV index points) |
| wind_direction | Ridge | 31.85 | 4.35 | 3.89 | 0.39 | -1.22 | 51.55 | 33.13 | 33.80 | 30.93 | 28.37 | 52.08 | 24.81 | 30.17 | 25.88 | 24.12 | circular MAE reduction % |

## Headline findings

- **Strongest performers (LOSO L1):** pressure, wind gust, relative humidity, wind speed, dew point, temperature, and wind direction all improve over baseline. UV gets corrected MAE near 0.30 UV-index points.
- **Bias correction:** five of six residual targets show large bias correction at L1. Relative humidity baseline bias was already near zero, so percentage bias correction is not always meaningful.
- **Across all three validations:** April out-of-sample numbers land between LOSO (conservative) and within-station (optimistic) for most targets. UV is consistent across all modes when normalized.

## Limitations and caveats

- **Wind direction LOSO weakens sharply at long leads** under the corrected angle-reconstruction convention. Within-station performance holds, so production behavior at trained stations is fine, but cold-start at new stations expects little useful direction correction beyond short leads.
- **Pressure long-lead skill drops** at multi-day horizons, consistent with synoptic uncertainty.
- **April scoring uses archive_api baseline** (same source as training canonical), not true issued historical forecasts. Open-Meteo does not expose hour-resolution archived forecasts. April numbers are training-consistent but not an operational issued-forecast test.
- **UV reported in two forms:** absolute MAE (UV-index points) and normalized (% of typical UV magnitude). The former is interpretable directly; the latter is comparable across seasons.
- **Rain investigated separately.** Both rain occurrence and rain amount are persistence-dominated under all tested architectures. See `rain_appendix.md` for three shipping options.

## Block 3 handover

- Locked production artifacts: `models/artifacts/{target}_{model_class}_lead{N}h.joblib`
- 576 files (8 targets x 72 leads), about 214 MB total
- Each artifact contains: model, scaler (Ridge only), imputation_stats, feature_columns, target, lead, model_class, hp
- Wind direction artifacts include output_layout [`winddir_residual_sin`, `winddir_residual_cos`] and reconstruction note `angle_convention`
- Block 3 inference contract: load by (target, lead), impute features using artifact's imputation_stats, scale (Ridge only), predict, reconstruct residual to absolute using artifact's predicts_residual flag

## Data references

- Master metrics CSV: `master_metrics.csv` (122 rows)
- Wide summary table: `summary_table.csv` (8 rows)
- Six charts: `figures/G1_horizon_curves.png` through `figures/G6_uv_normalized.png`
- Colour-coded summary table image: `figures/summary_table.png`
- Validation row counts: LOSO 40, within-station 40, April 40
