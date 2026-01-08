-- Create a centroid table for counties.
-- This is handy later for:
-- - mapping labels
-- - looking up a "representative point" for a county
-- - API responses without sending full polygons

CREATE TABLE IF NOT EXISTS ref.ref_county_centroid AS
SELECT
  county_fips,
  county_name,
  state_fips,
  ST_Y(ST_Centroid(geom)) AS centroid_lat,
  ST_X(ST_Centroid(geom)) AS centroid_lon
FROM ref.ref_county;

-- Index for fast lookups by county_fips
CREATE INDEX IF NOT EXISTS idx_ref_county_centroid_fips
ON ref.ref_county_centroid (county_fips);