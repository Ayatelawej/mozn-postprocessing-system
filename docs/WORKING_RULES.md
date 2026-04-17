# Working rules

## Data rules
- Keep `data/raw/` immutable.
- Convert large pooled tables to parquet early.
- Preserve source provenance for every merged dataset.
- Save reusable processed tables instead of rebuilding from notebooks every time.

## Modeling rules
- Train by target family, not with one single catch-all assumption.
- Keep an explicit benchmark sheet for every target.
- Sequence models must compete against classical baselines, not replace them automatically.
- Derived metrics should be produced from corrected core variables where appropriate.

## Deployment rules
- Batch inference should be supported.
- API-ready outputs should have stable columns and metadata.
- Fallback to base forecast must exist when a target model is rejected or unavailable.
