with
    rennamed as (
        select
            cast(territoryid as int) as pk_territory
            , cast(countryregioncode as varchar) as fk_country_region
            , cast(name as varchar) as territory_name
            , cast(group_name as varchar) as territory_group_name
            , cast(salesytd as float) as territory_sales_ytd
            , cast(saleslastyear as float) as territory_sales_last_year
            --costytd
            --costlastyear
            --rowguid
            --modifieddate
        from {{ source('sap', 'salesterritory') }}
    )
    
select *
from rennamed