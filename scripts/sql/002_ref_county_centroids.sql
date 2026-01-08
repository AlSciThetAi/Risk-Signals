-- Create a centroid table for counties.
-- This is handy later for:
-- - mapping labels
-- - looking up a "representative point" for a county
-- - API responses without sending full polygons

-- If an older version exists as a TABLE, remove it so we can create a VIEW.
DROP VIEW  IF EXISTS ref.ref_county_centroid;

-- Create/replace the VIEW (always up to date with ref.ref_county)
CREATE OR REPLACE VIEW ref.ref_county_centroid AS
SELECT
  county_fips,
  county_name,
  state_fips,
  ST_Y(ST_Centroid(geom)) AS centroid_lat,
  ST_X(ST_Centroid(geom)) AS centroid_lon
FROM ref.ref_county;