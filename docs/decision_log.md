# Decision Log

Tracks decision changes after the original Phase 0 lock and the Final Delivery Plan. Used to regenerate the plan document at end of Block 1 and to provide context for future builds and retrains.

## 2026-04-28 — Wind gust un-excluded from v1 training

**Original decision (Phase 0):** Wind gust excluded from initial build. Reasoning was that the prototype showed gust correction worsened MAE (+4.87%), and V3 on a second station showed inconsistent patterns suggesting station-setup faults. The plan deferred gust until a fault detection system was operational.

**Revised:** Fault detection system is out of scope for the July 31 delivery (estimated availability Oct-Nov 2026, possibly not part of the application). The plan's spirit — "let the benchmark decide what ships" — was contradicted by pre-excluding a target based on prototype evidence from a single station. Wind gust is now treated like every other target. Block 2 benchmark with all 26 stations either accepts the model, accepts with restrictions, or flags it as fallback_only via the standard acceptance threshold logic.

**Files updated:**
- `src/postprocessing/targets/residuals.py`: gust added to `CORE_RESIDUAL_PAIRS`
- `configs/models.yaml`: `target_model_map.wind_gust` set to `[ridge, hist_gradient_boosting]`
- `configs/training.yaml`: `v1_training_subset.experimental_gust.models` set to `[ridge, hist_gradient_boosting]`, deferred-note removed
- `tests/unit/test_config.py`: gust added to allowed targets set; locked-decision assertion updated

**Files still to update:**
- `Mozn_AI_Final_Delivery_Plan.docx` decision #6 (regenerated end of Block 1)

## 2026-04-29 — Pressure target uses MSL elevation correction (revised same day, see below)

**Original approach:** Subtract `pressure_max_hpa` (and min, avg) directly from `base_surface_pressure_hpa`.

**Problem identified (May 4 diagnostic):** Pooled diagnostic across all 26 stations showed pressure offsets correlated with station elevation. A station at 815 m showed +88 hPa offset. The textbook hydrostatic correction (~12 hPa per 100 m) reproduces this exactly. Initial conclusion: convention mismatch between station's at-elevation surface pressure and Open-Meteo's MSL.

**Initial revision:** Apply elevation correction to station pressure (× 0.12 hPa/m) before subtracting `base_msl_pressure_hpa`.

### Correction (later same day, May 6)

The initial revision was wrong — applied the correction in the wrong direction.

**Empirical finding from canonical-table dry run:** Raw `pressure_max_hpa` from FT0360 stations is approximately constant (~1013 hPa) across stations spanning 4-815 m elevation. At 815 m, true surface pressure should be ~924 hPa; the fact that the station reports 1013 hPa means the FT0360 firmware applies elevation correction internally and reports MSL pressure to the data acquisition layer. Stations report MSL, not surface.

**Reinterpretation of May 4 diagnostic:** the elevation-correlated offset existed because the diagnostic compared station-MSL (constant across elevation) against `base_surface_pressure_hpa` (varies with elevation by definition). The correlation was an artifact of the convention mismatch between the two sources, not a calibration issue requiring correction on the station side.

**Final approach:** Subtract `base_msl_pressure_hpa` directly from raw station pressure. No elevation correction on either side. Residuals across the three test stations (4 m, 144 m, 815 m) are all in the -3 to -5 hPa range — small, comparable, and consistent with genuine calibration drift.

**Files updated (final state):**
- `src/postprocessing/targets/pressure.py`: simple direct subtraction against `base_msl_pressure_hpa`
- `tests/unit/test_targets_pressure.py`: tests reflect direct subtraction; no elevation logic
- `docs/data/DATA_SOURCES.md`: pressure section rewritten with corrected interpretation

**Files still to update:**
- The four "anomalous" stations originally flagged for manual review (IBIRAL3, IJABAL13, IJABAL14, IMURQU5) need re-evaluation under the corrected math. Some flags may disappear; others may intensify. Defer to full 26-station diagnostic run.
