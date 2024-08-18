with
    stg_products as (
        select *
        from {{ ref('stg_sap__products') }}
    )

    , stg_subcat as (
        select *
        from {{ ref('stg_sap__product_sub_categories') }}
    )

    , stg_cat as (
        select *
        from {{ ref('stg_sap__product_categories') }}
    )

    , join_products as (
        select
            stg_products.pk_product
            , stg_products.fk_product_sub_category
            , stg_subcat.fk_product_category
            , stg_products.product_name
            , stg_subcat.product_sub_category_name
            , stg_cat.product_category_name
        from stg_products
        left join stg_subcat
            on stg_products.fk_product_sub_category = stg_subcat.pk_product_sub_category
        left join stg_cat
            on stg_subcat.fk_product_category = stg_cat.pk_product_category
    )

select *
from join_products
