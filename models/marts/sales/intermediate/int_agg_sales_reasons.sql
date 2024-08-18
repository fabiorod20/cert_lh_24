with
    dim_reasons as (
        select *
        from {{ ref('dim_sales_reasons') }}
    )

    , agg_reasons as (
        select
            dim_reasons.fk_sales_order
            , array_to_string(array_agg(dim_reasons.fk_sales_reason), ', ') as fk_sales_reason
            , array_to_string(array_agg(dim_reasons.sales_reason_name), ', ') as sales_reason_name
            , array_to_string(array_agg(dim_reasons.sales_reason_type), ', ') as sales_reason_type
        from dim_reasons
        group by
            dim_reasons.fk_sales_order
    )

select *
from agg_reasons
