from __future__ import annotations

import numpy as np
import pandas as pd


REGISTRY_PATH = "data/manifests/station_registry.csv"
LIBYA_LAT = (19.0, 34.0)
LIBYA_LON = (9.0, 26.0)
DEFAULT_MATCH_KM = 1.5
DEFAULT_SANITY_KM = 25.0


def load_registry(path=REGISTRY_PATH):
    r = pd.read_csv(path)
    return r[["station_id", "station_name", "latitude", "longitude", "elevation_m"]].copy()


def haversine_km(lat1, lon1, lat2, lon2):
    radius = 6371.0088
    p1 = np.radians(lat1)
    p2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlmb = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2.0) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dlmb / 2.0) ** 2
    return 2.0 * radius * np.arcsin(np.sqrt(a))


def _in_bbox(lat, lon):
    return LIBYA_LAT[0] <= lat <= LIBYA_LAT[1] and LIBYA_LON[0] <= lon <= LIBYA_LON[1]


def resolve_one(uuid, lat, lon, backend_elev, registry, *, prefer_backend=False, match_km=DEFAULT_MATCH_KM, sanity_km=DEFAULT_SANITY_KM):
    dists = haversine_km(lat, lon, registry["latitude"].to_numpy(), registry["longitude"].to_numpy())
    j = int(np.argmin(dists))
    dist = float(dists[j])
    reg = registry.iloc[j]
    matched = dist <= match_km
    flags = []

    if not _in_bbox(lat, lon):
        flags.append("coords_out_of_libya")

    if prefer_backend:
        lat_use, lon_use = float(lat), float(lon)
        elev_use = float(backend_elev) if backend_elev is not None and not pd.isna(backend_elev) else np.nan
        elev_source = "backend"
        if pd.isna(elev_use) or elev_use == 0.0:
            flags.append("backend_elevation_invalid")
            if matched:
                elev_use = float(reg["elevation_m"])
                elev_source = "registry"
            else:
                elev_use = np.nan
                elev_source = "open-meteo-pending"
    else:
        if matched:
            lat_use, lon_use = float(reg["latitude"]), float(reg["longitude"])
            elev_use = float(reg["elevation_m"])
            elev_source = "registry"
        else:
            lat_use, lon_use = float(lat), float(lon)
            elev_use = np.nan
            elev_source = "open-meteo-pending"

    if matched and dist > sanity_km:
        flags.append("match_beyond_sanity_km")
    if not matched:
        flags.append("unmatched_new_station")

    return {
        "station_id": uuid,
        "matched_wu_id": reg["station_id"] if matched else None,
        "matched_name": reg["station_name"] if matched else None,
        "distance_km": round(dist, 3),
        "latitude": lat_use,
        "longitude": lon_use,
        "elevation_m": elev_use,
        "elevation_source": elev_source,
        "needs_openmeteo_elevation": bool(elev_source == "open-meteo-pending"),
        "flags": ";".join(flags) if flags else "",
    }


def resolve_stations(active, registry, *, prefer_backend=False, match_km=DEFAULT_MATCH_KM, sanity_km=DEFAULT_SANITY_KM):
    rows = [
        resolve_one(
            s["station_id"],
            s["latitude"],
            s["longitude"],
            s.get("elevation"),
            registry,
            prefer_backend=prefer_backend,
            match_km=match_km,
            sanity_km=sanity_km,
        )
        for s in active
    ]
    return pd.DataFrame(rows)


def print_reconciliation(resolutions):
    print(f"{'uuid':40}{'wu_id':>12}{'dist_km':>9}{'elev_m':>8}{'elev_src':>20}  flags")
    for _, r in resolutions.iterrows():
        uuid = str(r["station_id"])[:38]
        wu = str(r["matched_wu_id"]) if not pd.isna(r["matched_wu_id"]) else "-"
        elev = "nan" if pd.isna(r["elevation_m"]) else f"{r['elevation_m']:.0f}"
        print(f"{uuid:40}{wu:>12}{r['distance_km']:>9.2f}{elev:>8}{str(r['elevation_source']):>20}  {r['flags']}")
