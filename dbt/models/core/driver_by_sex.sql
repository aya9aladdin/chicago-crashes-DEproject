with driver as (select * from {{ ref("stg_people") }} where person_type = 'DRIVER')

select sex, (count(sex) / (select count(*) from driver)) * 100 as percentage
from (select sex from driver)
group by 1
order by percentage
