with
    stg_order_reasons as (
        select *
        from {{ ref('stg_sap__order_sales_reasons') }}
    )

    , stg_reasons as (
        select *
        from {{ ref('stg_sap__sales_reasons') }}
    )

    , join_reasons as (
        select
            stg_order_reasons.fk_sales_order
            , stg_order_reasons.fk_sales_reason
            , stg_reasons.sales_reason_name
            , stg_reasons.sales_reason_type
        from stg_order_reasons
        left join stg_reasons
            on stg_order_reasons.fk_sales_reason = stg_reasons.pk_sales_reason
    )

select *
from join_reasons
