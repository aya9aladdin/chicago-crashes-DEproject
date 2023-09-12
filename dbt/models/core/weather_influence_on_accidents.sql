with
    weather as (
        select * from {{ ref("stg_crashes") }} where prim_contributory_cause = 'WEATHER'
    )

select
    weather_condition,
    count(*) / (
        select count(*)
        from {{ ref("stg_crashes") }} s
        where s.weather_condition = w.weather_condition
    )
    * 100 as crashe_likehood
from weather w
group by 1
order by 2 desc
