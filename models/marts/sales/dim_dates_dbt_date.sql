with 
    raw_generated_data as (
        {{ dbt_date.get_date_dimension("2010-01-01", "2015-12-31") }}
    )

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['date_day'])}} as sk_dim_date
            , *
        from raw_generated_data
    )

select *
from create_sk
