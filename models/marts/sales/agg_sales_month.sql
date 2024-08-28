with
    fact_sales as (
        select *
        from {{ ref('fact_sales') }}
    )

    , agg_sales_month as (
        select
            date_trunc('month', fact_sales.dt_order) as sale_month
            , count (distinct fact_sales.fk_sales_order) as total_orders
            , round(avg(fact_sales.final_price), 2) as avg_final_price
            , round(sum(fact_sales.product_qty), 2) as sum_product_qty
            , round(sum(fact_sales.final_price), 2) as sum_final_price
        from fact_sales
        group by
            date_trunc('month', fact_sales.dt_order)
    )

    , moving_avg as (
        select
            *
            , round(avg(total_orders) over (
                order by sale_month
                rows between 2 preceding and current row
            ), 2) as moving_avg_3_months_total_orders
            , round(avg(avg_final_price) over (
                order by sale_month
                rows between 2 preceding and current row
            ), 2) as moving_avg_3_months_avg_final_price
            , round(avg(sum_product_qty) over (
                order by sale_month
                rows between 2 preceding and current row
            ), 2) as moving_avg_3_months_sum_product_qty
            , round(avg(sum_final_price) over (
                order by sale_month
                rows between 2 preceding and current row
            ), 2) as moving_avg_3_months__sum_final_price
        from agg_sales_month
        order by sale_month
    )

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['sale_month'])}} as sk_agg_sales_month
            , *
        from moving_avg
    )

select *
from create_sk
order by sale_month