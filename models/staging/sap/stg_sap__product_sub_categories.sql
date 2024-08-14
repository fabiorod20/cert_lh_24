with
    rennamed as (
        select
            cast(productsubcategoryid as int) as pk_product_sub_category
            , cast(productcategoryid as int) as fk_product_category
            , cast(name as varchar) as product_sub_category_name
            --rowguid
            --modifieddate
        from {{ source('sap', 'productsubcategory') }}
    )
    
select *
from rennamed