with
    int_reasons as (
        select *
        from {{ ref('int_sales_reasons') }}
    )

    , agg_reasons as (
        select
            int_reasons.fk_sales_order
            , array_to_string(array_agg(int_reasons.fk_sales_reason), ', ') as fk_sales_reason
            , array_to_string(array_agg(int_reasons.sales_reason_name), ', ') as sales_reason_name
            , array_to_string(array_agg(int_reasons.sales_reason_type), ', ') as sales_reason_type
        from int_reasons
        group by
            int_reasons.fk_sales_order
    )

    , reasons_columns as (
        select
            *
            , case
                when sales_reason_name like '%Price%' then true
                else false
            end as eh_price
            , case
                when sales_reason_name like '%Manufacturer%' then true
                else false
            end as eh_manufacturer
            , case
                when sales_reason_name like '%Quality%' then true
                else false
            end as eh_quality
            , case
                when sales_reason_name like '%On Promotion%' then true
                else false
            end as eh_on_promotion
            , case
                when sales_reason_name like '%Review%' then true
                else false
            end as eh_review
            , case
                when sales_reason_name like '%Other%' then true
                else false
            end as eh_other
            , case
                when sales_reason_name like '%Television  Advertisement%' then true
                else false
            end as eh_television_advertisement
        from agg_reasons
    )

    , types_columns as (
        select
            *
            , case
                when sales_reason_type like '%Other%' then true
                else false
            end as eh_type_other
            , case
                when sales_reason_type like '%Promotion%' then true
                else false
            end as eh_type_promotion
            , case
                when sales_reason_type like '%Marketing%' then true
                else false
            end as eh_type_marketing
        from reasons_columns
    )

select *
from types_columns
