from __future__ import annotations

import sys
import warnings

import pandas as pd

from postprocessing.inference.frame import build_inference_frame
from postprocessing.training.feature_selection import features_for
from postprocessing.training.preparation import prepare_for_target


APRIL_PATH = r"data/processed/april2026_eval_frame_v3.parquet"
TARGETS = ("temperature", "pressure", "uv", "wind_direction")
LEADS = (1, 24, 72)
TOL = 1e-9


def main():
    warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

    april = pd.read_parquet(APRIL_PATH)
    april["valid_time_utc"] = pd.to_datetime(april["valid_time_utc"], utc=True)

    prepared = {}
    common_keys = None
    for tgt in TARGETS:
        for h in LEADS:
            prep = prepare_for_target(april, tgt, h)
            prepared[(tgt, h)] = prep
            keys = set(zip(prep["station_id"], prep["issue_time_utc"], strict=False))
            common_keys = keys if common_keys is None else common_keys & keys

    if not common_keys:
        print("ERROR: no common prepared row across target/lead checks")
        sys.exit(1)

    station, issue_t = sorted(common_keys)[len(common_keys) // 2]
    sdf = april[april["station_id"] == station].sort_values("valid_time_utc")
    print(f"station={station}  issue_time={issue_t}")

    inf = build_inference_frame(april, issue_t)
    inf_s = inf[inf["station_id"] == station].reset_index(drop=True)
    print(f"issue-time rows returned: {len(inf)}  (this station: {len(inf_s)})")
    if len(inf_s) != 1:
        print("ERROR: expected exactly one row for the station at issue time")
        sys.exit(1)

    all_ok = True
    for tgt in TARGETS:
        for h in LEADS:
            feats = features_for(tgt, h)
            missing = [c for c in feats if c not in inf_s.columns]
            if missing:
                print(f"  {tgt} L{h}: MISSING COLUMNS {missing[:5]}")
                all_ok = False
                continue
            row = inf_s.reindex(columns=feats).iloc[0]
            nan_pre = sum(1 for c in feats if pd.isna(row[c]))

            prep = prepared[(tgt, h)]
            prow = prep[(prep["station_id"] == station) & (prep["issue_time_utc"] == issue_t)]
            if len(prow) == 0:
                print(f"  {tgt} L{h}: n_feat={len(feats)}  cols_present=OK  nan_pre={nan_pre}  parity=NO_TRAIN_ROW")
                all_ok = False
                continue
            b = prow.reindex(columns=feats).iloc[0]
            diffs = []
            for c in feats:
                av, bv = row[c], b[c]
                if pd.isna(av) and pd.isna(bv):
                    continue
                if (pd.isna(av) != pd.isna(bv)) or (abs(float(av) - float(bv)) > TOL):
                    diffs.append(c)
            status = "OK" if not diffs else f"MISMATCH {diffs[:5]}"
            all_ok = all_ok and not diffs
            print(f"  {tgt} L{h}: n_feat={len(feats)}  cols_present=OK  nan_pre={nan_pre}  parity={status}")

    last_t = sdf["valid_time_utc"].iloc[-1]
    inf_last = build_inference_frame(april, last_t)
    inf_last_s = inf_last[inf_last["station_id"] == station]
    prep_last = prepare_for_target(april, "temperature", 72)
    kept = len(prep_last[(prep_last["station_id"] == station) & (prep_last["issue_time_utc"] == last_t)])
    print(f"\nfilter-bypass check at latest issue_time={last_t}:")
    print(f"  inference keeps the issue row: {len(inf_last_s) == 1}  |  prepare_for_target keeps it: {kept == 1}")

    print(f"\nPARITY {'PASS' if all_ok else 'FAIL'}")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
