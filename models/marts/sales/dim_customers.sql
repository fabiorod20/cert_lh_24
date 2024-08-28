with
    stg_costumers as (
        select *
        from {{ ref('stg_sap__customers') }}
    )

    , int_people as (
        select *
        from {{ ref('int_people') }}
    )

    , stg_stores as (
        select *
        from {{ ref('stg_sap__stores') }}
    )

    , join_customers as (
        select
            stg_costumers.*
            , int_people.person_name as customer_name
            , int_people.email_address as customer_email
            , int_people.phone_number as costumer_phone_number
            , int_people.phone_number_type as costumer_phone_number_type
            , stg_stores.store_name as customer_store
        from stg_costumers
        left join int_people
            on stg_costumers.fk_person = int_people.pk_person
        left join stg_stores
            on stg_costumers.fk_store = stg_stores.pk_store
    )

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['pk_customer'])}} as sk_dim_customer
            , *
        from join_customers
    )

select *
from create_sk

