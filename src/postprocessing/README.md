# Source package layout

This package is structured to keep production logic modular.

- `ingestion/`: raw station and baseline data loading
- `qc/`: broad range checks, missingness, spike detection
- `alignment/`: lag and time-alignment handling
- `health/`: warm-up and station-health rules
- `preprocessing/`: canonical train / inference table builders
- `features/`: feature backbones and target-specific builders
- `targets/`: target-family logic
- `models/`: model wrappers by family
- `training/`: train loops, search plans, experiment orchestration
- `evaluation/`: leakage-safe validation and diagnostics
- `inference/`: runtime prediction assembly
- `registry/`: manifests, artifact metadata, acceptance decisions
- `api/`: backend-facing service layer
- `monitoring/`: runtime health and drift tracking
- `pipelines/`: end-to-end orchestration wrappers
- `utils/`: shared helpers
