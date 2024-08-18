with
    rennamed as (
        select
            cast(businessentityid as int) as fk_person
            , cast(phonenumbertypeid as int) as fk_phone_number_type
            , cast(phonenumber as varchar) as phone_number
            --modifieddate
        from {{ source('sap', 'personphone') }}
    )
    
select *
from rennamed
