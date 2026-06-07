# Rain Forecasting - Investigation Appendix

## Decision

Rain occurrence and rain amount are **excluded from v1 shipping models**. They are presented to Mozn as three implementation options for the product team to choose based on business priorities.

## What we found

### Rain occurrence (binary classification, probability output)

- HGB classifier achieves F1=0.868 at L1, 0.385 at L72
- Naive persistence (predict rain at T+h = rain at T) F1=0.911 at L1, 0.269 at L72
- **HGB beats persistence at L6+ by 0.03-0.15 F1 points; persistence beats HGB at L1 by 0.04**
- Binary rain signal is post-processable, just lead-dependent

### Rain amount (conditional regression on positive-rain rows)

- HGB MAE 0.79mm at L1 vs persistence MAE 0.28mm (HGB 2.8x worse than persistence)
- HGB without persistence features: still loses to persistence at all leads
- Feature importance showed model relied on rain lags about 100x more than validity-time forecast features
- **No tested model architecture beats naive persistence at any horizon**

## Root-cause hypothesis

1. ECMWF/Open-Meteo precipitation amount forecasting is genuinely limited in arid regions with convective rain.
2. Tipping-bucket PWS rain gauges have quantization noise on amount; occurrence is robust to this noise but amount is not.
3. The positive-rate suggests the canonical's `rain_occurrence` may include drizzle and trace amounts that do not represent meaningful rain events.

Both target and baseline appear compromised by data-quality issues outside the scope of post-processing. A higher-quality rain dataset would be needed for v2 rain models.

## Three shipping options for Mozn

### Option 1 - Don't ship rain in v1

Mozn's app shows rain via Open-Meteo's raw forecast or skips rain entirely. Our system ships the other 8 targets. Simplest and honest about what works.

### Option 2 - Ship persistence for rain (recommended for v1)

Implement on Mozn's side in about 5 lines: `predicted_rain_at_T+h = station_rain_at_T`. No model artifact needed from us. Works well for L1-L6 and degrades gracefully at long leads.

### Option 3 - Hybrid (persistence at L1, HGB at L6+)

For rain occurrence: persistence at lead=1, HGB at longer leads. For rain amount: persistence at all leads. More complex inference but captures HGB's L24-L72 advantage on occurrence.

**Our recommendation: Option 2 (or Option 1).** The hybrid's L6+ wins on rain occurrence are small in operational terms. Simpler shipping plus a future v2 with better data is cleaner.

## What v2 would need

- Cleaner rain amount targets: calibrated tipping-bucket gauges, radar-derived hourly rainfall, or a paid commercial precipitation product
- Stricter `rain_occurrence` definition, such as >=0.5mm/hour instead of any non-zero amount
- Possibly switch baseline forecast for rain target only

## Diagnostics retained

- `reports/diagnostics/rain_persistence_diagnostic/`
- `reports/diagnostics/rain_experiments_ab/`
- `reports/diagnostics/rain_hgb_classifier_sweep/`
- `reports/diagnostics/rain_amount_hgb_sweep/`
