with
    rennamed as (
        select
            cast(countryregioncode as varchar) as pk_country
            , cast(name as varchar) as country_name
            --modifieddate
        from {{ source('sap', 'countryregion') }}
    )
    
select *
from rennamed