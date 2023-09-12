with
    people_data as (select * from {{ ref("stg_people") }}),

    crashes_data as (select * from {{ ref("stg_crashes") }}),

    unit as (
        select c.prim_contributory_cause, age_group,
        from crashes_data c
        inner join people_data p on c.crash_record_id = p.crash_record_id
        where
            person_type = 'DRIVER'
            and prim_contributory_cause not in ('UNABLE TO DETERMINE', 'NOT APPLICABLE')
    )

select
    prim_contributory_cause,
    (
        select count(*)
        from unit i
        where
            i.prim_contributory_cause = o.prim_contributory_cause
            and age_group = 'child'
    ) / (
        select count(*)
        from unit i
        where i.prim_contributory_cause = o.prim_contributory_cause
    )
    * 100 as child_ratio,

    (
        select count(*)
        from unit i
        where
            i.prim_contributory_cause = o.prim_contributory_cause and age_group = 'teen'
    ) / (
        select count(*)
        from unit i
        where i.prim_contributory_cause = o.prim_contributory_cause
    )
    * 100 as teen_ratio,

    (
        select count(*)
        from unit i
        where
            i.prim_contributory_cause = o.prim_contributory_cause
            and age_group = 'young adult'
    ) / (
        select count(*)
        from unit i
        where i.prim_contributory_cause = o.prim_contributory_cause
    )
    * 100 as young_adults_ratio,

    (
        select count(*)
        from unit i
        where
            i.prim_contributory_cause = o.prim_contributory_cause
            and age_group = 'old adult'
    ) / (
        select count(*)
        from unit i
        where i.prim_contributory_cause = o.prim_contributory_cause
    )
    * 100 as old_adults_ratio,

    (
        select count(*)
        from unit i
        where
            i.prim_contributory_cause = o.prim_contributory_cause
            and age_group = 'eldery'
    ) / (
        select count(*)
        from unit i
        where i.prim_contributory_cause = o.prim_contributory_cause
    )
    * 100 as eldery_ratio,
from unit o
group by 1
