# Target modules

Flat layout. One module per target family. Each module is a pure function over a dataframe.

## Modules

- `residuals.py`: temperature, relative humidity, dew point, wind speed, wind gust (residual = station − baseline)
- `pressure.py`: three candidate pressure residuals (max, min, avg), MSL-corrected for elevation
- `wind_direction.py`: circular residual (encoded as sin/cos for two-output regression)
- `rain.py`: occurrence (binary, NaN-preserving), amount log1p (regression), amount mm (diagnostic)
- `derived.py`: heat index and wind chill (NWS formulas, applied at training and inference time)

Each module:
- returns a copy of the input dataframe with target columns added
- skips gracefully when input columns are absent
- propagates NaN row-wise (the trainability gates filter NaN-target rows before training)

Per-target acceptance rules and evaluation metrics live in `evaluation/` and the model registry, not here. Target construction stays simple.
