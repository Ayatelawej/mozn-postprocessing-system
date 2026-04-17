# Phase 0 Lock

This file freezes the project before implementation begins.

## Locked mission

Build the final-form post-processing system so it can attempt all planned targets
from the start on pooled data, while still keeping target-specific routing and
acceptance checks.

## Locked target universe

### Core correction targets
- temperature
- relative humidity
- dew point
- wind speed

### Experimental but included from the start
- pressure
- wind gust
- UV / solar-related target(s)

### Specialized targets included from the start
- rain occurrence
- rain amount
- wind direction

### Derived outputs
- wind chill
- heat index

## Locked engineering rules

1. The final project must run from a clean clone.
2. No hardcoded local machine paths are allowed.
3. All raw data remains immutable.
4. All models must be versioned with manifests and metrics.
5. All targets must have separate evaluation and keep/scrap logic.
6. Sequence models are allowed from day one, but they do not replace clean baselines.
7. Backend deployment is part of the project scope, not an afterthought.

## Locked infrastructure expectations

- `.venv` for Python environment
- `.env` for local runtime variables
- requirements split by purpose
- reusable `configs/`
- `src/` package layout
- tests, scripts, deployment, and GitHub workflow folders from day one

## Locked completion condition

The project is only considered phase-complete when:
- training is reproducible,
- inference is reproducible,
- outputs are backend-friendly,
- tests run,
- deployment skeleton exists,
- target benchmark results are documented.
