with
    stg_orders as (
        select *
        from {{ ref('stg_sap__sales_order_headers') }}
    )

    , stg_order_details as (
        select *
        from {{ ref('stg_sap__sales_order_details') }}
    )

    , dim_dates as (
        select *
        from {{ ref('dim_dates_dbt_date') }}
    )

    , join_order_details as (
        select
            stg_order_details.pk_sales_order_detail
            , stg_order_details.fk_sales_order
            , stg_order_details.fk_product
            , stg_orders.fk_customer
            , stg_orders.fk_sales_person
            , stg_orders.fk_territory
            , stg_orders.fk_address_ship
            , stg_orders.fk_credit_card
            , stg_orders.dt_order
            , stg_orders.dt_ship
            , stg_orders.dt_due
            , dim_dates.day_of_week_name
            , dim_dates.day_of_year
            , dim_dates.week_of_year
            , dim_dates.month_name
            , dim_dates.quarter_of_year
            , dim_dates.year_number
            , stg_orders.order_status
            , stg_order_details.product_qty
            , stg_order_details.unit_price
            , stg_order_details.unit_price_discount
            , stg_orders.eh_order_online
            , stg_orders.order_sub_total
            , stg_orders.order_taxa_mt
            , stg_orders.order_freight
            , stg_orders.order_total
        from stg_order_details
        left join stg_orders
            on stg_order_details.fk_sales_order = stg_orders.pk_sales_order
        left join dim_dates
            on stg_orders.dt_order = dim_dates.date_day
    )

select *
from join_order_details
