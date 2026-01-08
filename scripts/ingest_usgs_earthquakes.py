"""
USGS Earthquakes -> Bronze ingestion (idempotent upserts)

What this script does:
1) Calls the USGS Earthquake API (GeoJSON)
2) Parses each event ("feature") into a row
3) Upserts into bronze.usgs_earthquakes using quake_id as the primary key

Why we do it this way:
- APIs can return overlapping data across runs
- We want re-runs to be safe (idempotent): no duplicates, updates allowed
- Storing raw JSON helps debugging and future schema improvements
"""

import json
from datetime import datetime, timedelta, timezone

import requests
from sqlalchemy import text

from ingest.db import get_engine


# USGS FDSN event API (supports many filters; we'll use a time window)
USGS_ENDPOINT = "https://earthquake.usgs.gov/fdsnws/event/1/query"


def to_usgs_iso(dt: datetime) -> str:
    """
    Convert a datetime into the ISO format USGS accepts.

    USGS accepts strings like:
      2026-01-08T12:34:56Z

    We force UTC and replace "+00:00" with "Z" for readability.
    """
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def fetch_usgs_geojson(days_back: int, min_magnitude: float) -> dict:
    """
    Fetch earthquakes from USGS for the last N days.

    We keep this as a separate function to make the script easier to test later.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days_back)

    params = {
        "format": "geojson",
        "starttime": to_usgs_iso(start),
        "endtime": to_usgs_iso(end),
        "minmagnitude": min_magnitude,
        "orderby": "time",
        # Generous limit so we don't truncate results for typical MVP windows.
        # You can tighten/parameterize this later.
        "limit": 20000,
    }

    resp = requests.get(USGS_ENDPOINT, params=params, timeout=60)
    resp.raise_for_status()  # raises an error for non-200 responses
    return resp.json()


def parse_feature(feature: dict) -> dict | None:
    """
    Convert one USGS GeoJSON feature into a dict matching bronze.usgs_earthquakes.

    USGS feature structure:
    - feature["id"] -> quake_id (unique)
    - feature["properties"] -> metadata (mag, place, time, etc.)
    - feature["geometry"]["coordinates"] -> [lon, lat, depth_km]

    Returns None if required fields are missing.
    """
    quake_id = feature.get("id")
    props = feature.get("properties") or {}
    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates") or [None, None, None]

    lon, lat, depth_km = coords[0], coords[1], coords[2]

    # USGS time is milliseconds since epoch
    t_ms = props.get("time")

    # Basic validation: skip malformed entries
    if quake_id is None or t_ms is None or lon is None or lat is None:
        return None

    time_utc = datetime.fromtimestamp(t_ms / 1000.0, tz=timezone.utc)

    return {
        "quake_id": quake_id,
        "time_utc": time_utc,
        "mag": props.get("mag"),
        "place": props.get("place"),
        "lon": float(lon),
        "lat": float(lat),
        "depth_km": float(depth_km) if depth_km is not None else None,
        # Store raw feature as JSON text; we cast to jsonb in SQL
        "raw": json.dumps(feature),
    }


def upsert_rows(rows: list[dict]) -> int:
    """
    Insert rows into bronze.usgs_earthquakes with ON CONFLICT upsert.

    Idempotency:
    - quake_id is the primary key
    - If quake_id already exists, we UPDATE the row instead of inserting a duplicate
    """
    engine = get_engine()

    upsert_sql = text(
        """
        INSERT INTO bronze.usgs_earthquakes
          (quake_id, time_utc, mag, place, lon, lat, depth_km, geom, raw, ingested_at, updated_at)
        VALUES
          (:quake_id, :time_utc, :mag, :place, :lon, :lat, :depth_km,
           ST_SetSRID(ST_Point(:lon, :lat), 4326),
           CAST(:raw AS jsonb), now(), now())
        ON CONFLICT (quake_id) DO UPDATE SET
          time_utc = EXCLUDED.time_utc,
          mag = EXCLUDED.mag,
          place = EXCLUDED.place,
          lon = EXCLUDED.lon,
          lat = EXCLUDED.lat,
          depth_km = EXCLUDED.depth_km,
          geom = EXCLUDED.geom,
          raw = EXCLUDED.raw,
          updated_at = now();
        """
    )

    # engine.begin() opens a transaction and commits automatically if no errors occur
    inserted = 0
    with engine.begin() as conn:
        for r in rows:
            conn.execute(upsert_sql, r)
            inserted += 1

    return inserted


def main(days_back: int = 30, min_magnitude: float = 0.0):
    """
    Orchestrates the ingestion process:
    - Fetch
    - Parse
    - Upsert
    """
    data = fetch_usgs_geojson(days_back=days_back, min_magnitude=min_magnitude)
    features = data.get("features", [])

    print(f"Fetched {len(features)} USGS features (days_back={days_back}, min_mag={min_magnitude})")

    rows: list[dict] = []
    skipped = 0

    for f in features:
        row = parse_feature(f)
        if row is None:
            skipped += 1
            continue
        rows.append(row)

    print(f"Parsed {len(rows)} rows, skipped {skipped} malformed features")

    upserted = upsert_rows(rows)
    print(f"Upserted {upserted} rows into bronze.usgs_earthquakes")


if __name__ == "__main__":
    main()