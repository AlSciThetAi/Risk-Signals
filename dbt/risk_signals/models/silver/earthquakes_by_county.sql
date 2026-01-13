{{ config(
    materialized='table',
    post_hook=[
      "create index if not exists idx_silver_quakes_county on {{ this }} (county_fips)",
      "create index if not exists idx_silver_quakes_time   on {{ this }} (time_utc)",
      "create index if not exists idx_silver_quakes_geom   on {{ this }} using gist (geom)"
    ]
) }}

select
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
from {{ source('bronze', 'usgs_earthquakes') }} as q
join {{ source('ref', 'ref_county') }} as c
  on st_contains(c.geom, q.geom)
