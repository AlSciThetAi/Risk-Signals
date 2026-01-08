-- silver.earthquakes_by_county
-- Purpose:
--   Assign each earthquake point to a county polygon.
-- Notes:
--   We build this as a TABLE (not a view) so it's easy to query and index.
--   We DROP+CREATE so rerunning db-migrate refreshes it cleanly.

DROP TABLE IF EXISTS silver.earthquakes_by_county;

CREATE TABLE silver.earthquakes_by_county AS
SELECT
  q.quake_id,
  q.time_utc,
  q.mag,
  q.depth_km,
  q.place,
  q.lon,
  q.lat,
  q.geom,
  c.county_fips,
  c.county_name,
  c.state_fips
FROM bronze.usgs_earthquakes q
JOIN ref.ref_county c
  ON ST_Contains(c.geom, q.geom);

-- Helpful indexes for common queries
CREATE INDEX IF NOT EXISTS idx_silver_quakes_county ON silver.earthquakes_by_county (county_fips);
CREATE INDEX IF NOT EXISTS idx_silver_quakes_time   ON silver.earthquakes_by_county (time_utc);
CREATE INDEX IF NOT EXISTS idx_silver_quakes_geom   ON silver.earthquakes_by_county USING GIST (geom);