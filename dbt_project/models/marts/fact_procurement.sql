-- Grain: üks rida ettevõtte ja hankelepingu kohta
-- Ettevõtte versioon (ja seega sektor) valitakse lepingu sõlmimise kuupäeva järgi
select
    coalesce(c.company_id, '-1')                    as company_id,
    to_char(p.award_date, 'YYYYMMDD')::int          as date_id,
    p.contract_id                                   as procurement_id,
    p.awarded_contract_amount
from {{ ref('stg_procurements') }} p
left join {{ ref('dim_company') }} c
       on c.registration_code = p.registration_code
      and p.award_date between c.valid_from and c.valid_to
