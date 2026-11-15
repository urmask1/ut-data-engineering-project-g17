-- EMTA kvartaliandmed. field_of_activity muutub ajas.
select
    id                                                  as emta_id,
    company_registration_code                           as registration_code,
    coalesce(nullif(trim(field_of_activity), ''), 'Teadmata') as sector_name, -- Eemaldab tühikud ja asendab tühjad väärtused 'Teadmata'
    year,
    quarter,
    make_date(year, (quarter - 1) * 3 + 1, 1)           as quarter_start, -- Arvutab kvartali alguse kp
    revenue::numeric(18, 2)                             as revenue,
    employee_count
from {{ source('raw', 'raw_emta_data') }}
