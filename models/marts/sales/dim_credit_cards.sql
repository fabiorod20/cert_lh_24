with
    stg_people_credit_cards as (
        select *
        from {{ ref('stg_sap__people_credit_cards') }}
    )

    , int_people as (
        select *
        from {{ ref('int_people') }}
    )

    , stg_credit_cards as (
        select *
        from {{ ref('stg_sap__credit_cards') }}
    )

    , join_people_credit_cards as (
        select
            stg_people_credit_cards.*
            , int_people.person_name as person_credit_card_name
        from stg_people_credit_cards
        left join int_people
            on stg_people_credit_cards.pk_person_credit_card = int_people.pk_person
    )

    , join_credit_card as (
        select
            stg_credit_cards.pk_credit_card
            , join_people_credit_cards.pk_person_credit_card as fk_person_credit_card
            , join_people_credit_cards.person_credit_card_name
            , stg_credit_cards.credit_card_type
    from stg_credit_cards
    left join join_people_credit_cards
        on stg_credit_cards.pk_credit_card = join_people_credit_cards.fk_credit_card
    )

select *
from join_credit_card
