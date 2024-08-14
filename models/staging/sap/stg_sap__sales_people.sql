with
    rennamed as (
        select
            cast(businessentityid as int) as pk_sales_person
            , cast(territoryid as int) as fk_territory
            , cast(salesquota as int) as sales_quota
            , cast(bonus as int) as bonus_sales_person
            , cast(commissionpct as float) as commission_pct_sales_person
            , cast(salesytd as float) as sales_ytd_sales_person
            , cast(saleslastyear as float) as sales_last_year_sales_person
            -- rowguid
            -- modifieddate
        from {{ source('sap', 'salesperson') }}
    )

select *
from rennamed