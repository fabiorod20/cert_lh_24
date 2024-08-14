with
    rennamed as (
        select
            cast(salesreasonid as int) as pk_sales_reason
            , cast(name as varchar) as sales_reason_name
            , cast(reasontype as varchar) as sales_reason_type
            --modifieddate
        from {{ source('sap', 'salesreason') }}
    )
    
select *
from rennamed