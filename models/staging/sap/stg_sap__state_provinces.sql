with
    rennamed as (
        select
            cast(stateprovinceid as int) as pk_state_province
            , cast(countryregioncode as varchar) as fk_country_region
            , cast(territoryid as varchar) as fk_territory
            , cast(name as varchar) as state_province_name
            --stateprovincecode
            --isonlystateprovinceflag
            --rowguid
            --modifieddate
        from {{ source('sap', 'stateprovince') }}
    )
    
select *
from rennamed