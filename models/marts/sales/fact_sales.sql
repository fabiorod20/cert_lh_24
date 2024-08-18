with
    int_sales as (
        select *
        from {{ ref('int_sales_order_details') }}
    )

    , int_reasons as (
        select *
        from {{ ref('int_agg_sales_reasons') }}
    )

    , dim_customers as (
        select *
        from {{ ref('dim_customers') }}
    )

    , dim_credit_cards as (
        select *
        from {{ ref('dim_credit_cards') }}
    )

    , dim_locations as (
        select *
        from {{ ref('dim_locations') }}
    )

    , dim_products as (
        select *
        from {{ ref('dim_products') }}
    )

    , dim_sales_people as (
        select *
        from {{ ref('dim_sales_people') }}
    )

    , joined as (
        select
            int_sales.pk_sales_order_detail
            , int_sales.fk_sales_order
            , int_sales.fk_customer
            , dim_sales_people.fk_territory
            , int_sales.fk_address_ship
            , dim_locations.fk_state_province
            , dim_locations.fk_country_region
            , int_sales.fk_credit_card
            , int_reasons.fk_sales_reason
            , dim_customers.fk_store
            , dim_credit_cards.fk_person_credit_card
            , int_sales.fk_product
            , dim_products.fk_product_sub_category
            , dim_products.fk_product_category
            , int_sales.fk_sales_person
            , int_sales.dt_order
            , int_sales.dt_ship
            , int_sales.dt_due
            , dim_sales_people.dt_birth_sales_person
            , dim_sales_people.dt_hire_sales_person
            , int_sales.order_status
            , int_sales.product_qty
            , int_sales.unit_price
            , int_sales.unit_price_discount
            , int_sales.eh_order_online
            , int_sales.order_sub_total
            , int_sales.order_taxa_mt
            , int_sales.order_freight
            , int_sales.order_total
            , int_reasons.sales_reason_name
            , int_reasons.sales_reason_type
            , int_reasons.eh_price
            , int_reasons.eh_manufacturer
            , int_reasons.eh_quality
            , int_reasons.eh_on_promotion
            , int_reasons.eh_review
            , int_reasons.eh_television_advertisement
            , int_reasons.eh_other
            , int_reasons.eh_type_promotion
            , int_reasons.eh_type_marketing
            , int_reasons.eh_type_other
            , dim_customers.customer_name
            , dim_customers.store_customer
            , dim_credit_cards.person_credit_card_name
            , dim_credit_cards.credit_card_type
            , dim_locations.city_name
            , dim_locations.state_province_name
            , dim_locations.country_name
            , dim_locations.distribution_center
            , dim_locations.spatial_location
            , dim_products.product_name
            , dim_products.product_sub_category_name
            , dim_products.product_category_name
            , dim_sales_people.name_sales_person
            , dim_sales_people.job_title_sales_person
            , dim_sales_people.gender_sales_person
            , dim_sales_people.eh_employee_sales_person
            , dim_sales_people.sales_quota
            , dim_sales_people.bonus_sales_person
            , dim_sales_people.commission_pct_sales_person
            , dim_sales_people.sales_ytd_sales_person
            , dim_sales_people.sales_last_year_sales_person
            , dim_sales_people.territory_name
            , dim_sales_people.territory_group_name
            , dim_sales_people.territory_sales_ytd
            , dim_sales_people.territory_sales_last_year
        from int_sales
        left join int_reasons
            on int_sales.fk_sales_order = int_reasons.fk_sales_order
        left join dim_customers
            on int_sales.fk_customer = dim_customers.pk_customer
        left join dim_credit_cards
            on int_sales.fk_credit_card = dim_credit_cards.pk_credit_card
        left join dim_locations
            on int_sales.fk_address_ship = dim_locations.pk_address
        left join dim_products
            on int_sales.fk_product = dim_products.pk_product
        left join dim_sales_people
            on int_sales.fk_sales_person = dim_sales_people.pk_sales_person
    )

select *
from joined
