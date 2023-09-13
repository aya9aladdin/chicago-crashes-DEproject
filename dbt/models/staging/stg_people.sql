{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        unique_key="person_id",
        partition_by={
            "field": "crash_date",
            "data_type": "timestamp",
            "granularity": "year",
        },
    )
}}


with
    people_data as (
        select
            person_id,
            person_type,
            crash_record_id,
            cast(crash_date as timestamp) as crash_date,
            cast(seat_no as numeric) as seat_no,
            city,
            state,
            sex,
            age,
            drivers_license_state,
            drivers_license_class,
            safety_equipment,
            airbag_deployed,
            ejection,
            injury_classification,
            driver_action,
            driver_vision,
            physical_condition,
            pedpedal_action,
            pedpedal_visibility,
            bac_result,
            cast(bac_result_value as float64) as bac_result_value,
            {{ cellphone("cell_phone_use") }} as cell_phone_use,
            {{ age_group("age") }} as age_group
        from {{ source("crashes", "people_data") }}
    )

select *
from people_data

{% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- (uses > to include records whose timestamp occurred since the last run of this
    -- model)
    where crash_date > (select max(crash_date) from {{ this }})

{% endif %}
