with
    rennamed as (
        select
            cast(customerid as int) as pk_customer
            , cast(personid as int) as fk_person
            , cast(storeid as int) as fk_store
            , cast(territoryid as int) as fk_territory
            --rowguid
            --modifieddate
        from {{ source('sap', 'customer') }}
    )
    
select *
from rennamed
