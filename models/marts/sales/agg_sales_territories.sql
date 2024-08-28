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
            , count (distinct fact_sales.fk_sales_order) as total_orders
            , round(avg(fact_sales.final_price), 2) as avg_final_price
            , round(sum(fact_sales.product_qty), 2) as sum_product_qty
            , round(sum(fact_sales.final_price), 2) as sum_final_price
        from fact_sales
        group by
            fact_sales.fk_territory
            , fact_sales.territory_name
            , fact_sales.territory_group_name
            , fact_sales.territory_sales_ytd
            , fact_sales.territory_sales_last_year
    )

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['fk_territory'])}} as sk_agg_sales_territory
            , *
        from agg_sales_person
    )

select *
from create_sk
where fk_territory is not null
