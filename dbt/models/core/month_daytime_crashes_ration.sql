with crashes as (select * from {{ ref("stg_crashes") }})

select
    crash_month,
    (
        select count(*)
        from crashes i
        where i.crash_month = o.crash_month and day_time = 'midnight'
    )
    / (select count(*) from crashes i where i.crash_month = o.crash_month)
    * 100 as midnight_ratio,

    (
        select count(*)
        from crashes i
        where i.crash_month = o.crash_month and day_time = 'early morning'
    )
    / (select count(*) from crashes i where i.crash_month = o.crash_month)
    * 100 as early_morning_ratio,

    (
        select count(*)
        from crashes i
        where i.crash_month = o.crash_month and day_time = 'morning'
    )
    / (select count(*) from crashes i where i.crash_month = o.crash_month)
    * 100 as morning_ratio,

    (
        select count(*)
        from crashes i
        where i.crash_month = o.crash_month and day_time = 'afternoon'
    )
    / (select count(*) from crashes i where i.crash_month = o.crash_month)
    * 100 as afternoon_ratio,

    (
        select count(*)
        from crashes i
        where i.crash_month = o.crash_month and day_time = 'evening'
    )
    / (select count(*) from crashes i where i.crash_month = o.crash_month)
    * 100 as evening_ratio,

    (
        select count(*)
        from crashes i
        where i.crash_month = o.crash_month and day_time = 'night'
    )
    / (select count(*) from crashes i where i.crash_month = o.crash_month)
    * 100 as night_ratio,

from crashes o
group by 1
order by 1
