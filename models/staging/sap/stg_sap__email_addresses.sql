with
    rennamed as (
        select
            cast(emailaddressid as int) as pk_email_address
            , cast(businessentityid as int) as fk_person
            , cast(emailaddress as varchar) as email_address
            --rowguid
            --modifieddate
        from {{ source('sap', 'emailaddress') }}
    )
    
select *
from rennamed