from __future__ import annotations

import sys

import pandas as pd

from postprocessing.inference.station_metadata import (
    load_registry,
    print_reconciliation,
    resolve_stations,
)


def main():
    reg = load_registry()

    active = []
    for i, row in reg.iterrows():
        active.append({
            "station_id": f"fake-{i:02d}-{row['station_id']}",
            "latitude": round(float(row["latitude"]), 2),
            "longitude": round(float(row["longitude"]), 2),
            "elevation": 0,
        })
    active.append({
        "station_id": "3a30aa25-80af-40ba-b73a-62b969b5d282",
        "latitude": 32.78,
        "longitude": 12.86,
        "elevation": 0,
    })
    active.append({
        "station_id": "new-desert-001",
        "latitude": 27.0,
        "longitude": 17.0,
        "elevation": 0,
    })
    active.append({
        "station_id": "bad-coords-001",
        "latitude": 5.0,
        "longitude": 5.0,
        "elevation": 0,
    })

    res = resolve_stations(active, reg, prefer_backend=False)
    print_reconciliation(res)
    print()

    ok = True

    sub = res[res["station_id"].str.startswith("fake-")]
    selfmatch = 0
    for _, r in sub.iterrows():
        own = r["station_id"].split("-", 2)[2]
        if r["matched_wu_id"] == own and r["distance_km"] <= 1.5 and r["elevation_source"] == "registry":
            selfmatch += 1
    print(f"rounded-registry self-match: {selfmatch}/{len(sub)}")
    ok = ok and selfmatch == len(sub)

    rm = res[res["station_id"] == "3a30aa25-80af-40ba-b73a-62b969b5d282"].iloc[0]
    rm_ok = rm["matched_wu_id"] == "IJANZO3" and abs(rm["elevation_m"] - 8) < 1e-9 and rm["elevation_source"] == "registry"
    print(f"Roaya Maya -> IJANZO3 @ elev 8: {rm_ok}  (got {rm['matched_wu_id']}, elev={rm['elevation_m']}, {rm['distance_km']} km)")
    ok = ok and rm_ok

    nd = res[res["station_id"] == "new-desert-001"].iloc[0]
    nd_ok = pd.isna(nd["matched_wu_id"]) and bool(nd["needs_openmeteo_elevation"]) and "unmatched_new_station" in nd["flags"]
    print(f"new desert station flagged new + needs Open-Meteo elev: {nd_ok}")
    ok = ok and nd_ok

    bad = res[res["station_id"] == "bad-coords-001"].iloc[0]
    bad_ok = "coords_out_of_libya" in bad["flags"]
    print(f"out-of-Libya coords flagged: {bad_ok}")
    ok = ok and bad_ok

    print(f"\nRESOLVER CHECK {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
