with
    rennamed as (
        select
            cast(businessentityid as int) as bid_employee
            , cast(jobtitle as varchar) as job_title
            , cast(birthdate as date) as dt_birth
            , cast(gender as varchar) as gender
            , cast(hiredate as date) as dt_hire
            , cast(currentflag as varchar) as eh_employee
            -- nationalidnumber
            -- loginid
            -- maritalstatus
            -- salariedflag
            -- vacationhours
            -- sickleavehours
            -- rowguid
            -- modifieddate
            -- organizationnode
        from {{ source('sap', 'employee') }}
    )
    
select *
from rennamed