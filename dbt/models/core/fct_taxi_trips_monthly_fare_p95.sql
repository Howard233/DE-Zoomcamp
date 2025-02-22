{{ config(materialized="table") }}

with
    fare_table as (
        select year, month, service_type, fare_amount
        from {{ ref("fact_trips") }}
        where
            fare_amount > 0
            and trip_distance > 0
            and lower(payment_type_description) in ('cash', 'credit card')
    )

select
    year,
    month,
    service_type,
    percentile_cont(fare_amount, 0.97) over (
        partition by service_type, year, month
    ) as p97,
    percentile_cont(fare_amount, 0.95) over (
        partition by service_type, year, month
    ) as p95,
    percentile_cont(fare_amount, 0.9) over (
        partition by service_type, year, month
    ) as p90
from fare_table

-- Answer to Q6
-- select * from `dbt_DEZoomcamp_hw.fct_taxi_trips_monthly_fare_p95` 
-- where year = 2020 and month = 4 and service_type = 'Green'
-- limit 1; -- {p97:55.0, p95: 45.0, p90:26.5}

-- select * from `dbt_DEZoomcamp_hw.fct_taxi_trips_monthly_fare_p95` 
-- where year = 2020 and month = 4 and service_type = 'Yellow'
-- limit 1; -- {p97:31.5, p95: 25.5, p90:19.0}