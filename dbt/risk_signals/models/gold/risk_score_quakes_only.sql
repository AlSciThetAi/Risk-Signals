{{ config(
    materialized='table',
    post_hook=[
      "create index if not exists idx_gold_risk_score_fips on {{ this }} (county_fips)"
    ]
) }}

select
  county_fips,
  county_name,
  state_fips,
  centroid_lat,
  centroid_lon,
  quakes_14d,

  case
    when quakes_14d = 0 then 0
    when quakes_14d between 1 and 2 then 20
    when quakes_14d between 3 and 5 then 40
    when quakes_14d between 6 and 10 then 60
    else 80
  end as risk_score,

  case
    when quakes_14d = 0 then 'No earthquakes (M>=2.5) detected in last 14 days'
    when quakes_14d = 1 then '1 earthquake (M>=2.5) detected in last 14 days'
    else quakes_14d::text || ' earthquakes (M>=2.5) detected in last 14 days'
  end as drivers

from {{ ref('quakes_14d_by_county') }}