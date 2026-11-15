-- Grain: üks rida ettevõtte ja kvartali kohta
select
    coalesce(c.company_id, '-1')                    as company_id,
    to_char(e.quarter_start, 'YYYYMMDD')::int       as date_id,
    e.revenue,
    e.employee_count
from {{ ref('stg_emta') }} e
left join {{ ref('dim_company') }} c
       on c.registration_code = e.registration_code
      and e.quarter_start between c.valid_from and c.valid_to
