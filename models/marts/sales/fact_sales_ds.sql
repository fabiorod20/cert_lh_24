with
    int_sales as (
        select *
        from {{ ref('int_sales_order_details') }}
    )

    , dim_sales_reasons as (
        select *
        from {{ ref('dim_sales_reasons') }}
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
            int_sales.dt_order
            --int_sales.pk_sales_order_detail
            --, int_sales.fk_sales_order
            --, int_sales.fk_customer
            --, dim_sales_people.fk_territory
            --, int_sales.fk_address_ship
            --, dim_locations.fk_state_province
            --, dim_locations.fk_country_region
            --, int_sales.fk_credit_card
            --, dim_sales_reasons.fk_sales_reason
            --, dim_customers.fk_store
            --, dim_credit_cards.fk_person_credit_card
            --, int_sales.fk_product
            --, dim_products.fk_product_sub_category
            --, dim_products.fk_product_category
            --, int_sales.fk_sales_person
            --, int_sales.dt_ship
            --, int_sales.dt_due
            --, int_sales.day_of_week_name
            --, int_sales.day_of_year
            --, int_sales.week_of_year
            --, int_sales.month_name
            --, int_sales.quarter_of_year
            --, int_sales.year_number
            --, dim_sales_people.dt_birth_sales_person
            --, dim_sales_people.dt_hire_sales_person
            --, int_sales.order_status
            --, case
            --    when int_sales.eh_order_online = 'true'
            --        then 'Online'
            --    else 'Vendedor'
            --end as eh_order_online
            --, case
            --    when dim_sales_reasons.sales_reason_name is null
            --        then 'Não Identificado'
            --    else dim_sales_reasons.sales_reason_name
            --end as sales_reason_name
            --, dim_sales_reasons.sales_reason_type
            --, dim_customers.customer_name
            --, dim_customers.customer_email
            --, dim_customers.costumer_phone_number
            --, dim_customers.costumer_phone_number_type
            , case
                when dim_customers.customer_store is null
                    then 'Online'
                else dim_customers.customer_store
            end as customer_store
            --, dim_credit_cards.person_credit_card_name
            --, case
            --    when dim_credit_cards.credit_card_type is null
            --        then 'Outro'
            --    else dim_credit_cards.credit_card_type
            --end as credit_card_type
            --, dim_locations.city_name
            --, dim_locations.state_province_name
            --, dim_locations.country_name
            , dim_locations.distribution_center
            , dim_locations.eh_usa
            , dim_products.product_name
            --, dim_products.product_sub_category_name
            --, dim_products.product_category_name
            --, case
            --    when dim_sales_people.name_sales_person is null
            --        then 'Online'
            --    else dim_sales_people.name_sales_person
            --end as name_sales_person
            --, dim_sales_people.job_title_sales_person
            --, dim_sales_people.gender_sales_person
            --, dim_sales_people.eh_employee_sales_person
            --, case
            --    when dim_sales_people.territory_name is null
            --        then 'Online'
            --    else dim_sales_people.territory_name
            --end as territory_name
            --, dim_sales_people.territory_group_name
            , int_sales.product_qty
            --, int_sales.unit_price
            --, int_sales.unit_price_discount
            --, int_sales.order_detail_sub_total
            --, int_sales.order_detail_taxa_mt
            --, int_sales.order_detail_freight
            , int_sales.final_price
            --, int_sales.order_sub_total
            --, int_sales.order_taxa_mt
            --, int_sales.order_freight
            --, int_sales.order_total
            --, dim_sales_reasons.eh_price
            --, dim_sales_reasons.eh_manufacturer
            --, dim_sales_reasons.eh_quality
            --, dim_sales_reasons.eh_on_promotion
            --, dim_sales_reasons.eh_review
            --, dim_sales_reasons.eh_television_advertisement
            --, dim_sales_reasons.eh_other
            --, dim_sales_reasons.eh_type_promotion
            --, dim_sales_reasons.eh_type_marketing
            --, dim_sales_reasons.eh_type_other
            --, dim_locations.spatial_location
            --, dim_sales_people.sales_quota
            --, dim_sales_people.bonus_sales_person
            --, dim_sales_people.commission_pct_sales_person
            --, dim_sales_people.sales_ytd_sales_person
            --, dim_sales_people.sales_last_year_sales_person
            --, dim_sales_people.territory_sales_ytd
            --, dim_sales_people.territory_sales_last_year
        from int_sales
        left join dim_sales_reasons
            on int_sales.fk_sales_order = dim_sales_reasons.fk_sales_order
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
