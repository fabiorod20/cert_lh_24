with
    rennamed as (
        select
            cast(salesorderid as int) as pk_sales_order
            , cast(customerid as int) as fk_customer
            , cast(salespersonid as int) as fk_sales_person
            , cast(territoryid as int) as fk_territory
            --, cast(billtoaddressid as int) as fk_address_bill
            , cast(shiptoaddressid as int) as fk_address_ship
            , cast(creditcardid as int) as fk_credit_card
            , cast(orderdate as date) as dt_order
            , cast(shipdate as date) as dt_ship
            , cast(duedate as date) as dt_due
            , cast(status as int) as order_status
            , cast(onlineorderflag as varchar) as eh_order_online
            , cast(subtotal as float) as order_sub_total
            , cast(taxamt as float) as order_taxa_mt
            , cast(freight as float) as order_freight
            , cast(totaldue as float) as order_total
            --revisionnumber
            --purchaseordernumber
            --accountnumber
            --shipmethodid
            --creditcardapprovalcode
            --currencyrateid
            --comment
            --rowguid
            --modifieddate
        from {{ source('sap', 'salesorderheader') }}
    )
    
select *
from rennamed
