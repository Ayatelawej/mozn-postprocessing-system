from __future__ import annotations

import pandas as pd

from postprocessing.training.data_loader import prepare_target_frame


def prepare_for_target(
    canonical: pd.DataFrame,
    target: str,
    lead: int,
) -> pd.DataFrame:
    framed = prepare_target_frame(canonical, target, leads=(lead,))
    if len(framed) == 0:
        raise RuntimeError(f"No trainable rows for target '{target}'")
    return framed
