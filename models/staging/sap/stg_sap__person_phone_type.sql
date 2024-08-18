with
    rennamed as (
        select
            cast(phonenumbertypeid as int) as pk_phone_number_type
            , cast(name as varchar) as phone_number_type
            --modifieddate
        from {{ source('sap', 'phonenumbertype') }}
    )

select *
from rennamed
