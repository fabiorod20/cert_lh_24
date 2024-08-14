with
    rennamed as (
        select
            cast(addressid as int) as pk_address
            , cast(stateprovinceid as varchar) as fk_state_province
            , cast(city as varchar) as city_name
            , cast(spatiallocation as varchar) as spatial_location
            --addressline1
            --addressline2
            --postalcode
            --rowguid
            --modifieddate
        from {{ source('sap', 'address') }}
    )
    
select *
from rennamed