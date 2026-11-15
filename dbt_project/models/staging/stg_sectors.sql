-- raw_sectors = detailne EMTAK tegevusvaldkond
select
    sector_code                 as field_code,
    trim(sector_name)           as field_name
from {{ source('raw', 'raw_sectors') }}
where sector_code is not null
