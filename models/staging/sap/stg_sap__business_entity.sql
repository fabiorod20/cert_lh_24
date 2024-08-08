with
    rennamed as (
        select
            cast(businessentityid as int) as bid_business_entity
            -- rowguid
            -- modifieddate
        from {{ source('sap', 'businessentity') }}
    )
    
select *
from rennamed