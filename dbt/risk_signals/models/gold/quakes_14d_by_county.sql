{{ config(
    materialized='table',
    post_hook=[
      "create index if not exists idx_gold_quakes_14d_fips on {{ this }} (county_fips)"
    ]
) }}

select
  c.county_fips,
  c.county_name,
  c.state_fips,
  c.centroid_lat,
  c.centroid_lon,
  coalesce(count(q.quake_id), 0) as quakes_14d
from {{ source('ref', 'ref_county_centroid') }} as c
left join {{ ref('earthquakes_by_county') }} as q
  on q.county_fips = c.county_fips
 and q.time_utc >= (now() at time zone 'utc') - interval '14 days'
 and q.mag >= 2.5
group by
  c.county_fips, c.county_name, c.state_fips, c.centroid_lat, c.centroid_lon