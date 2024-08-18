with
    fact_sales as (
        select *
        from {{ ref('fact_sales') }}
    )

    , agg_sales_person as (
        select
            fact_sales.fk_territory
            , fact_sales.territory_name
            , fact_sales.territory_group_name
            , fact_sales.territory_sales_ytd
            , fact_sales.territory_sales_last_year
            , avg(fact_sales.product_qty) as avg_product_qty
            , avg(fact_sales.unit_price) as avg_unit_price
            , avg(fact_sales.unit_price_discount) as avg_unit_price_discount
            , avg(fact_sales.order_sub_total) as avg_order_sub_total
            , avg(fact_sales.order_taxa_mt) as avg_order_taxa_mt
            , avg(fact_sales.order_freight) as avg_order_freight
            , avg(fact_sales.order_total) as avg_order_total
            , sum(fact_sales.product_qty) as sum_product_qty
            , sum(fact_sales.unit_price) as sum_unit_price
            , sum(fact_sales.unit_price_discount) as sum_unit_price_discount
            , sum(fact_sales.order_sub_total) as sum_order_sub_total
            , sum(fact_sales.order_taxa_mt) as sum_order_taxa_mt
            , sum(fact_sales.order_freight) as sum_order_freight
            , sum(fact_sales.order_total) as sum_order_total
        from fact_sales
        group by
            fact_sales.fk_territory
            , fact_sales.territory_name
            , fact_sales.territory_group_name
            , fact_sales.territory_sales_ytd
            , fact_sales.territory_sales_last_year
    )

select *
from agg_sales_person
where fk_territory is not null
