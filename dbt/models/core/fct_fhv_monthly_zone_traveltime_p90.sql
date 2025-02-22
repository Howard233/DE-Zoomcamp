{{ config(materialized="table") }}

with
    dim_fhv_trips as (
        select
            *,
            timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration
        from {{ ref('dim_fhv_trips') }}
    )

select 
    year,
    month,
    pulocationid,
    dolocationid,
    pickup_zone,
    dropoff_zone,
    percentile_cont(trip_duration, 0.9) over (
        partition by year, month, pulocationid, dolocationid
    ) as p90
from dim_fhv_trips

-- select * from `dbt_DEZoomcamp_hw.fct_fhv_monthly_zone_traveltime_p90`
-- where year = 2019 and month = 11 and pickup_zone = 'Yorkville East'
-- order by p90 desc limit 10;
-- -- LaGuardia Airport, Chinatown, Garment District
