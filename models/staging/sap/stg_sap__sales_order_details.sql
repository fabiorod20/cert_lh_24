with
    rennamed as (
        select
            cast(salesorderdetailid as int) as pk_sales_order_detail
            , cast(salesorderid as int) as fk_sales_order
            , cast(productid as int) as fk_product
            , cast(orderqty as int) as product_qty
            , cast(unitprice as float) as unit_price
            , cast(unitpricediscount as float) as unit_price_discount
            --carriertrackingnumber
            --specialofferid
            --rowguid
            --modifieddate
        from {{ source('sap', 'salesorderdetail') }}
    )
    
select *
from rennamed
