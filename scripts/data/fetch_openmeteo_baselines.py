from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from postprocessing.ingestion.openmeteo import (
    fetch_station_baseline,
    polite_sleep,
)
from postprocessing.ingestion.station_registry import load_stations
from postprocessing.utils.paths import get_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Open-Meteo baseline forecasts for all stations in the registry.",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Re-fetch even if cached parquets exist.",
    )
    parser.add_argument(
        "--only",
        nargs="*",
        default=None,
        help="Optional list of station_ids to fetch. Defaults to all 26.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = get_paths()
    paths.data.external_openmeteo_dir.mkdir(parents=True, exist_ok=True)

    stations = load_stations()
    if args.only:
        keep = set(args.only)
        stations = [s for s in stations if s.station_id in keep]
        missing = keep - {s.station_id for s in stations}
        if missing:
            print(f"Unknown station_ids: {sorted(missing)}", file=sys.stderr)
            return 1

    report: list[dict[str, object]] = []
    failed: list[str] = []

    for i, station in enumerate(stations, start=1):
        print(
            f"[{i}/{len(stations)}] {station.station_id} "
            f"({station.fetch_start.date()} -> {station.fetch_end.date()})",
            flush=True,
        )
        try:
            hourly_df, daily_df = fetch_station_baseline(
                station, force_refresh=args.force_refresh
            )
            report.append(
                {
                    "station_id": station.station_id,
                    "hourly_rows": len(hourly_df),
                    "daily_rows": len(daily_df),
                    "hourly_columns": sorted(hourly_df.columns.tolist()),
                    "daily_columns": sorted(daily_df.columns.tolist()),
                    "fetch_start": station.fetch_start.isoformat(),
                    "fetch_end": station.fetch_end.isoformat(),
                }
            )
        except Exception as exc:
            print(f"  FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
            failed.append(station.station_id)
            continue

        if i < len(stations):
            polite_sleep()

    manifest_path = (
        paths.data.manifests_dir
        / f"openmeteo_fetch_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.json"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "fetched_utc": datetime.now(timezone.utc).isoformat(),
                "stations_attempted": len(stations),
                "stations_succeeded": len(report),
                "stations_failed": failed,
                "report": report,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"\nManifest written: {manifest_path}")
    print(f"Succeeded: {len(report)} / {len(stations)}")
    if failed:
        print(f"Failed: {failed}")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
