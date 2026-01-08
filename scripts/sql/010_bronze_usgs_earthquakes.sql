-- Bronze: raw-ish USGS earthquakes feed (deduped by quake_id)

CREATE TABLE IF NOT EXISTS bronze.usgs_earthquakes (
  quake_id TEXT PRIMARY KEY,
  time_utc TIMESTAMPTZ NOT NULL,
  mag DOUBLE PRECISION,
  place TEXT,
  lon DOUBLE PRECISION NOT NULL,
  lat DOUBLE PRECISION NOT NULL,
  depth_km DOUBLE PRECISION,
  geom geometry(Point, 4326) NOT NULL,
  raw JSONB
);

CREATE INDEX IF NOT EXISTS idx_usgs_quakes_time ON bronze.usgs_earthquakes (time_utc);
CREATE INDEX IF NOT EXISTS idx_usgs_quakes_geom ON bronze.usgs_earthquakes USING GIST (geom);