-- gold.quakes_14d_by_county
-- Purpose:
--   Feature per county: number of earthquakes in the last 14 days.
--   Uses a LEFT JOIN so counties with 0 quakes still appear.

DROP TABLE IF EXISTS gold.quakes_14d_by_county;

CREATE TABLE gold.quakes_14d_by_county AS
SELECT
  c.county_fips,
  c.county_name,
  c.state_fips,
  c.centroid_lat,
  c.centroid_lon,
  COALESCE(COUNT(q.quake_id), 0) AS quakes_14d
FROM ref.ref_county_centroid c
LEFT JOIN silver.earthquakes_by_county q
  ON q.county_fips = c.county_fips
 AND q.time_utc >= (now() - interval '14 days')
GROUP BY
  c.county_fips, c.county_name, c.state_fips, c.centroid_lat, c.centroid_lon;

CREATE INDEX IF NOT EXISTS idx_gold_quakes_14d_fips
ON gold.quakes_14d_by_county (county_fips);