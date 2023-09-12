with crashes as (select * from {{ ref("stg_crashes") }})

select
    prim_contributory_cause,
    sum(injuries_incapacitating) / (
        select sum(injuries_total)
        from crashes i
        where i.prim_contributory_cause = o.prim_contributory_cause
    ) as severity_ratio
from crashes o
group by 1
order by 2 desc
