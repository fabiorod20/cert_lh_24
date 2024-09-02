with
    fact_sales as (
        select *
        from {{ ref('fact_sales') }}
    )

    , agg_sales_person as (
        select
            fact_sales.fk_sales_person
            , fact_sales.name_sales_person
            , fact_sales.job_title_sales_person
            , fact_sales.gender_sales_person
            , fact_sales.eh_employee_sales_person
            , fact_sales.sales_quota
            , fact_sales.bonus_sales_person
            , fact_sales.commission_pct_sales_person
            , fact_sales.sales_ytd_sales_person
            , fact_sales.sales_last_year_sales_person
            , count (distinct fact_sales.fk_sales_order) as total_orders
            , round(avg(fact_sales.final_price), 2) as avg_final_price
            , round(sum(fact_sales.product_qty), 2) as sum_product_qty
            , round(sum(fact_sales.final_price), 2) as sum_final_price
        from fact_sales
        group by
            fact_sales.fk_sales_person
            , fact_sales.name_sales_person
            , fact_sales.job_title_sales_person
            , fact_sales.gender_sales_person
            , fact_sales.eh_employee_sales_person
            , fact_sales.sales_quota
            , fact_sales.bonus_sales_person
            , fact_sales.commission_pct_sales_person
            , fact_sales.sales_ytd_sales_person
            , fact_sales.sales_last_year_sales_person
    )

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['fk_sales_person'])}} as sk_agg_sales_person
            , *
        from agg_sales_person
    )

select *
from create_sk
where fk_sales_person is not null
