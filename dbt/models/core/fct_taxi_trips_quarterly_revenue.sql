{{ config(materialized="table") }}

with
    quarterly_revenue as (
        select year, quarter, year_quarter, service_type, sum(total_amount) as quarterly_revenues
        from {{ ref('fact_trips') }}
        group by year, quarter, year_quarter, service_type
    )

select
    cur_rev_table.year,
    cur_rev_table.quarter,
    cur_rev_table.year_quarter,
    cur_rev_table.service_type,
    cur_rev_table.quarterly_revenues,
    (
        (cur_rev_table.quarterly_revenues - prev_rev_table.quarterly_revenues)
        / prev_rev_table.quarterly_revenues
    ) as quarterly_yoy_growth
from quarterly_revenue cur_rev_table
inner join
    quarterly_revenue prev_rev_table
    on cur_rev_table.year = prev_rev_table.year + 1
    and cur_rev_table.quarter = prev_rev_table.quarter
    and cur_rev_table.service_type = prev_rev_table.service_type


-- home work results
-- SELECT * FROM `dtc-de-course-447701.dbt_DEZoomcamp_hw.fct_taxi_trips_quarterly_revenue` 
-- where year = 2020 and service_type = 'Green'
-- order by quarterly_yoy_growth;
-- green worst 2020-Q2, best 2020-Q1
-- yellow worst 2020-Q2, best 2020 Q1