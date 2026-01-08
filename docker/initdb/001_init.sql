-- This file runs automatically ONLY the first time Postgres initializes
-- (i.e., when the Docker volume is empty).
-- If you already had a running DB volume before adding this file, you can
-- still run these commands manually in psql.

-- Enable PostGIS extension for geospatial types and functions (geometry, ST_Contains, etc.)
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create separate schemas to organize your “layers”
-- ref: reference data (counties, date spine, pipeline state)
-- bronze: raw ingested data
-- silver: cleaned/enriched data (typed, deduped, geocoded)
-- gold: analytics outputs (features, risk scores)
CREATE SCHEMA IF NOT EXISTS ref;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Track per-source watermarks so ingestion can be incremental + idempotent.
-- Example: store the most recent successful timestamp pulled from USGS.
CREATE TABLE IF NOT EXISTS ref.pipeline_state (
  -- e.g., 'noaa_storms', 'usgs_quakes', 'nasa_firms'
  source_name TEXT PRIMARY KEY,
  -- watermark used for incremental ingestion
  last_success_ts TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- optional: freeform notes / debugging breadcrumbs
  notes TEXT
);