with
    rennamed as (
        select
            cast(businessentityid as int) as pk_person
            , cast(persontype as varchar) as person_type
            , cast(firstname as varchar) as first_name
            , coalesce(cast(middlename as varchar), '') as middle_name
            , cast(lastname as varchar) as last_name
            , coalesce(cast(suffix as varchar), '') as suffix
            --namestyle
            --title
            --emailpromotion
            --additionalcontactinfo
            --demographics
            --rowguid
            --modifieddate
        from {{ source('sap', 'person') }}
    )
    
, full_name as (
        select
            pk_person
            , first_name || ' ' || middle_name || ' ' || last_name || ' ' || suffix as person_name
        from rennamed
    )  

select *
from full_name