with
    people_data as (select * from {{ ref("stg_people") }}),

    crashes_data as (select * from {{ ref("stg_crashes") }}),

    unit as (
        select c.prim_contributory_cause, sex,
        from crashes_data c
        inner join people_data p on c.crash_record_id = p.crash_record_id
        where
            person_type = 'DRIVER'
            and sex != 'X'
            and prim_contributory_cause not in ('UNABLE TO DETERMINE', 'NOT APPLICABLE')
    )

select
    prim_contributory_cause,
    (
        select count(*)
        from unit i
        where i.prim_contributory_cause = o.prim_contributory_cause and sex = 'F'
    ) / (
        select count(*)
        from unit i
        where i.prim_contributory_cause = o.prim_contributory_cause
    )
    * 100 as females_ratio,

    (
        select count(*)
        from unit i
        where i.prim_contributory_cause = o.prim_contributory_cause and sex = 'M'
    ) / (
        select count(*)
        from unit i
        where i.prim_contributory_cause = o.prim_contributory_cause
    )
    * 100 as males_ratio,

from unit o
group by 1
order by 2 desc
