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
            , round(avg(fact_sales.product_qty), 2) as avg_product_qty
            , round(avg(fact_sales.unit_price), 2) as avg_unit_price
            , round(avg(fact_sales.unit_price_discount), 2) as avg_unit_price_discount
            , round(avg(fact_sales.order_sub_total), 2) as avg_order_sub_total
            , round(avg(fact_sales.order_taxa_mt), 2) as avg_order_taxa_mt
            , round(avg(fact_sales.order_freight), 2) as avg_order_freight
            , round(avg(fact_sales.order_total), 2) as avg_order_total
            , round(sum(fact_sales.product_qty), 2) as sum_product_qty
            , round(sum(fact_sales.unit_price), 2) as sum_unit_price
            , round(sum(fact_sales.unit_price_discount), 2) as sum_unit_price_discount
            , round(sum(fact_sales.order_sub_total), 2) as sum_order_sub_total
            , round(sum(fact_sales.order_taxa_mt), 2) as sum_order_taxa_mt
            , round(sum(fact_sales.order_freight), 2) as sum_order_freight
            , round(sum(fact_sales.order_total), 2) as sum_order_total
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
