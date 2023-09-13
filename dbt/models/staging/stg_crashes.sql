{{ config(materialized="table", unique_key="crash_record_id") }}

with
    crashes_data as (
        select
            crash_record_id,
            cast(crash_date as timestamp) as crash_date,
            cast(posted_speed_limit as numeric) as posted_speed_limit,
            traffic_control_device,
            lighting_condition,
            first_crash_type,
            trafficway_type,
            alignment,
            roadway_surface_cond,
            road_defect,
            crash_type,
            hit_and_run_i,
            weather_condition,
            damage,
            prim_contributory_cause,
            sec_contributory_cause,
            dooring_i,
            cast(num_units as numeric) as num_units,
            most_severe_injury,
            cast(injuries_total as float64) as injuries_total,
            cast(injuries_fatal as float64) as injuries_fatal,
            cast(injuries_incapacitating as float64) as injuries_incapacitating,
            cast(injuries_non_incapacitating as float64) as injuries_non_incapacitating,
            cast(crash_hour as numeric) as crash_hour,
            cast(crash_day_of_week as numeric) as crash_day_of_week,
            cast(crash_month as numeric) as crash_month,
        from {{ source("crashes", "crashes_data") }}
    )

select *, {{ day_time("crash_hour") }} as day_time
from crashes_data
limit 1000000

{% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- (uses > to include records whose timestamp occurred since the last run of this
    -- model)
    where crash_date > (select max(crash_date) from {{ this }})

{% endif %}
