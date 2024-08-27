with
    stg_addresses as (
        select *
        from {{ ref('stg_sap__addresses') }}
    )

    , stg_provinces as (
        select *
        from {{ ref('stg_sap__state_provinces') }}
    )

    , stg_countries as (
        select *
        from {{ ref('stg_sap__country_regions') }}
    )

    , join_locations as (
        select
            stg_addresses.pk_address
            , stg_addresses.fk_state_province
            , stg_provinces.fk_country_region
            , stg_provinces.fk_territory
            , stg_addresses.city_name
            , stg_provinces.state_province_name
            , stg_countries.country_name
            , stg_addresses.spatial_location
        from stg_addresses
        left join stg_provinces
            on stg_addresses.fk_state_province = stg_provinces.pk_state_province
        left join stg_countries
            on stg_provinces.fk_country_region = stg_countries.pk_country
    )

    , create_distributions_center as (
        select
            *
            , case
                when fk_country_region = 'US'
                    then state_province_name
                else country_name
            end as distribution_center
            , case
                when fk_country_region = 'US'
                    then 'Estados Unidos'
                else 'Resto do Mundo'
            end as eh_usa
        from join_locations
    )

select *
from create_distributions_center
