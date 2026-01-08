"""
Load US county boundaries into PostGIS as ref.ref_county.

Why this matters:
- Once county polygons are in PostGIS, we can take any point event (earthquake/fire)
  and assign it to a county using a spatial join (ST_Contains).

Expected columns:
Most Census county files include:
- STATEFP  (2-digit state FIPS)
- COUNTYFP (3-digit county code within the state)
- NAME     (county name)
"""

import sys
from pathlib import Path

import geopandas as gpd
from sqlalchemy import text

from ingest.db import get_engine


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.load_ref_county <path_to_counties_file(.zip/.shp/.geojson)>")
        sys.exit(1)

    path = Path(sys.argv[1]).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    # Read shapefile/geojson/zip (GeoPandas can usually read zipped shapefiles directly)
    gdf = gpd.read_file(str(path))

    # Ensure CRS is WGS84 (EPSG:4326) so it matches typical lat/lon points we’ll ingest later
    if gdf.crs is None:
        gdf = gdf.set_crs(4326, allow_override=True)
    gdf = gdf.to_crs(4326)

    # Helper: find expected columns case-insensitively
    cols_lower = {c.lower(): c for c in gdf.columns}

    def pick(*names):
        for n in names:
            if n.lower() in cols_lower:
                return cols_lower[n.lower()]
        return None

    statefp = pick("STATEFP", "STATEFP20", "STATEFP10")
    countyfp = pick("COUNTYFP", "COUNTYFP20", "COUNTYFP10")
    name = pick("NAME", "NAMELSAD")

    if not statefp or not countyfp or not name:
        raise ValueError(
            "Could not find expected columns (STATEFP, COUNTYFP, NAME). "
            f"Found columns: {list(gdf.columns)}"
        )

    out = gdf[[statefp, countyfp, name, "geometry"]].copy()
    out.columns = ["state_fips", "county_fips_3", "county_name", "geom"]

    # Build a 5-digit county FIPS: state (2 digits) + county (3 digits)
    out["state_fips"] = out["state_fips"].astype(str).str.zfill(2)
    out["county_fips_3"] = out["county_fips_3"].astype(str).str.zfill(3)
    out["county_fips"] = out["state_fips"] + out["county_fips_3"]
    out = out.drop(columns=["county_fips_3"])

    # Fix some invalid geometries (common with polygons). buffer(0) is a simple repair trick.
    out["geom"] = out["geom"].buffer(0)

    # Write to PostGIS
    engine = get_engine()
    out = out.set_geometry("geom")

    # For reference data, replacing is OK (we can rebuild it any time)
    out.to_postgis("ref_county", engine, schema="ref", if_exists="replace", index=False)

    # Add primary key + indexes (big performance improvement for spatial joins later)
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE ref.ref_county ADD PRIMARY KEY (county_fips);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ref_county_geom ON ref.ref_county USING GIST (geom);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ref_county_state_fips ON ref.ref_county (state_fips);"))

    print("Loaded ref.ref_county successfully.")


if __name__ == "__main__":
    main()