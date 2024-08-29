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
            , stg_orders.eh_order_online
            , stg_orders.order_sub_total
            , stg_orders.order_taxa_mt
            , stg_orders.order_freight
            , stg_orders.order_total
            , stg_order_details.product_qty
            , stg_order_details.unit_price
            , stg_order_details.unit_price_discount
        from stg_order_details
        left join stg_orders
            on stg_order_details.fk_sales_order = stg_orders.pk_sales_order
        left join dim_dates
            on stg_orders.dt_order = dim_dates.date_day
    )

    , create_metrics as (
        select
            *
            , round(product_qty * unit_price * (1 - unit_price_discount), 2) as order_detail_sub_total
            , round(order_taxa_mt / count(*) over(partition by fk_sales_order), 2) as order_detail_taxa_mt
            , round(order_freight / count(*) over(partition by fk_sales_order), 2) as order_detail_freight
        from join_order_details
    )

    , create_final_price as (
        select
            *
            , round(order_detail_sub_total + order_detail_taxa_mt + order_detail_freight, 2) as final_price
        from create_metrics
    )

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['pk_sales_order_detail'])}} as sk_int_sales_order_detail
            , *
        from create_final_price
    )

select *
from create_sk
