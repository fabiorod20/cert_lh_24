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

select *
from agg_sales_person
where fk_sales_person is not null
