# Target modules

The repository is prepared for all planned targets from the start.

## Core residual correction
- temperature
- humidity
- dewpoint
- wind_speed

## Experimental
- pressure
- gust
- uv

## Specialized
- rain
- wind_direction

## Derived
- derived

Each target family should own:
- schema expectations,
- feature logic,
- model branch decisions,
- evaluation metrics,
- acceptance rules,
- inference reconstruction logic.
