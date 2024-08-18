with
    stg_phone as (
        select *
        from {{ ref('stg_sap__person_phone') }}
    )
    
    , stg_phone_type as (
        select *
        from {{ ref('stg_sap__person_phone_type') }}
    )

    , stg_email as (
        select *
        from {{ ref('stg_sap__email_addresses') }}
    )

    , stg_people as (
        select *
        from {{ ref('stg_sap__people') }}
    )

    , joined as (
        select
            stg_people.pk_person
            , stg_people.person_name
            , stg_email.email_address
            , stg_phone.phone_number
            , stg_phone_type.phone_number_type
        from stg_people
        left join stg_email
            on stg_people.pk_person = stg_email.fk_person
        left join stg_phone
            on stg_people.pk_person = stg_phone.fk_person
        left join stg_phone_type
            on stg_phone.fk_phone_number_type = stg_phone_type.pk_phone_number_type
    )

select *
from joined