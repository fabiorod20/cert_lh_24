with
    stg_costumers as (
        select *
        from {{ ref('stg_sap__customers') }}
    )

    , stg_people as (
        select *
        from {{ ref('stg_sap__people') }}
    )

    , stg_stores as (
        select *
        from {{ ref('stg_sap__stores') }}
    )

    , join_customers as (
        select
            stg_costumers.*
            , stg_people.person_name as customer_name
            , stg_stores.store_name as customer_store
        from stg_costumers
        left join stg_people
            on stg_costumers.fk_person = stg_people.pk_person
        left join stg_stores
            on stg_costumers.fk_store = stg_stores.pk_store
    )

select *
from join_customers
