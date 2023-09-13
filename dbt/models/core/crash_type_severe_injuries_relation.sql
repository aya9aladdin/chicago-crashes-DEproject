with crashes as (select * from {{ ref("stg_crashes") }})

select
    prim_contributory_cause,
    round(
        sum(injuries_incapacitating) / (
            select sum(injuries_total)
            from crashes i
            where
                i.prim_contributory_cause = o.prim_contributory_cause
                and not is_nan(injuries_incapacitating)
        )
        * 100,
        2
    ) as severity_ratio

from crashes o
where not is_nan(injuries_incapacitating)
group by 1
order by 2 desc
