with
    stg_employees as (
        select *
        from {{ ref('stg_sap__employees') }}
    )

    , stg_people as (
        select *
        from {{ ref('stg_sap__people') }}
    )

    , stg_territories as (
        select *
        from {{ ref('stg_sap__sales_territories') }}
    )

    , stg_sales_people as (
        select *
        from {{ ref('stg_sap__sales_people') }}
    )

    , join_employees as (
        select
            stg_employees.*
            , stg_people.person_name as employee_name
        from stg_employees
        left join stg_people
            on stg_employees.pk_employee = stg_people.pk_person
    )

    , join_sales_people as (
        select
            stg_sales_people.pk_sales_person
            , stg_sales_people.fk_territory
            , join_employees.dt_birth as dt_birth_sales_person
            , join_employees.dt_hire as dt_hire_sales_person
            , join_employees.employee_name as name_sales_person
            , join_employees.job_title as job_title_sales_person
            , join_employees.gender as gender_sales_person
            , join_employees.eh_employee as eh_employee_sales_person
            , stg_sales_people.sales_quota
            , stg_sales_people.bonus_sales_person
            , stg_sales_people.commission_pct_sales_person
            , stg_sales_people.sales_ytd_sales_person
            , stg_sales_people.sales_last_year_sales_person
            , stg_territories.territory_name
            , stg_territories.territory_group_name
            , stg_territories.territory_sales_ytd
            , stg_territories.territory_sales_last_year
    from stg_sales_people
    left join join_employees
        on stg_sales_people.pk_sales_person = join_employees.pk_employee
    left join stg_territories
        on stg_sales_people.fk_territory = stg_territories.pk_territory
    )

select *
from join_sales_people
