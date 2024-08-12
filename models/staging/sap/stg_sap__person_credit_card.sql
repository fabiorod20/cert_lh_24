with
    rennamed as (
        select
            cast(businessentityid as int) as pk_person_credit_card
            , cast(creditcardid as int) as fk_credit_card
            --modifieddate
        from {{ source('sap', 'personcreditcard') }}
    )
    
select *
from rennamed