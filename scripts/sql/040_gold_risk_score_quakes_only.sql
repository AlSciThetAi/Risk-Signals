-- MVP risk score using only earthquake signal.
-- We’ll add storms + precipitation later and weight them.

DROP TABLE IF EXISTS gold.risk_score_by_county;

CREATE TABLE gold.risk_score_by_county AS
SELECT
  county_fips,
  county_name,
  state_fips,
  centroid_lat,
  centroid_lon,
  quakes_14d,

  -- Simple scoring rule (tune later):
  -- 0 quakes -> 0
  -- 1-2 -> 20
  -- 3-5 -> 40
  -- 6-10 -> 60
  -- 11+ -> 80
  CASE
    WHEN quakes_14d = 0 THEN 0
    WHEN quakes_14d BETWEEN 1 AND 2 THEN 20
    WHEN quakes_14d BETWEEN 3 AND 5 THEN 40
    WHEN quakes_14d BETWEEN 6 AND 10 THEN 60
    ELSE 80
  END AS risk_score,

  -- “Why” explanation for the score (human readable)
  CASE
    WHEN quakes_14d = 0 THEN 'No earthquakes (M>=2.5) detected in last 14 days'
    WHEN quakes_14d = 1 THEN '1 earthquake (M>=2.5) detected in last 14 days'
    ELSE quakes_14d::text || ' earthquakes (M>=2.5) detected in last 14 days'
  END AS drivers

FROM gold.quakes_14d_by_county;

CREATE INDEX IF NOT EXISTS idx_gold_risk_score_fips
ON gold.risk_score_by_county (county_fips);