with
    rennamed as (
        select
            cast(creditcardid as int) as pk_credit_card
            , cast(cardtype as varchar) as credit_card_type
            --cardnumber
            --expmonth
            --expyear
            --modifieddate
        from {{ source('sap', 'creditcard') }}
    )
    
select *
from rennamed