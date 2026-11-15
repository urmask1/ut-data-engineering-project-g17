-- Valdkond = detailne EMTAK kood (raw_sectors, 1297 rida). Seotud dim_company'ga SCD1-na.
select field_code as field_id, field_name
from {{ ref('stg_sectors') }}

union all

select '-1', 'Teadmata'
