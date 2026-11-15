-- Sektor = EMTAK jagu (21 väärtust EMTA andmetest). Seotud dim_company'ga SCD2-na.
select
    {{ dbt_utils.generate_surrogate_key(['sector_name']) }}  as sector_id,
    sector_name
from (select distinct sector_name from {{ ref('stg_emta') }}) s
where sector_name <> 'Teadmata'

union all

select '-1', 'Teadmata'
