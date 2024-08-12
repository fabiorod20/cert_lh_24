with
    rennamed as (
        select
            cast(productcategoryid as int) as pk_product_category
            , cast(name as varchar) as product_category_name
            --rowguid
            --modifieddate
        from {{ source('sap', 'productcategory') }}
    )
    
select *
from rennamed