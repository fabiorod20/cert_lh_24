with
    rennamed as (
        select
            cast(productid as int) as pk_product
            , cast(productsubcategoryid as int) as fk_product_sub_category
            , cast(name as varchar) as product_name
            --productnumber
            --makeflag
            --finishedgoodsflag
            --color
            --safetystocklevel
            --reorderpoint
            --standardcost
            --listprice
            --size
            --sizeunitmeasurecode
            --weightunitmeasurecode
            --weight
            --daystomanufacture
            --productline
            --class
            --style
            --productmodelid
            --sellstartdate
            --sellenddate
            --discontinueddate
            --rowguid
            --modifieddate
        from {{ source('sap', 'product') }}
    )
    
select *
from rennamed