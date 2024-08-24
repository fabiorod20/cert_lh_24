with calculated_sum as (
    select 
        round(sum(order_total), 2) as total_sum
    from {{ ref('fact_sales') }}
    where date_trunc('month', dt_order) = '2013-10-01'
)

select *
from calculated_sum
where total_sum != 137060323.84