with
    rennamed as (
        select
            cast(businessentityid as int) as pk_store
            , cast(salespersonid as int) as fk_sales_person
            , cast(name as varchar) as store_name
            --demographics
            --rowguid
            --modifieddate
        from {{ source('sap', 'store') }}
    )
    
select *
from rennamed