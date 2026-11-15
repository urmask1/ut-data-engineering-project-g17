select
    contract_id,
    contract_issued_at                              as award_date,
    company_registration_code                       as registration_code,
    contract_award_amount::numeric(18, 2)           as awarded_contract_amount,
    case lower(trim(eu_funded))
        when 'no-eu-funds' then false
        when 'eu-funds'    then true
    end                                             as eu_funded
from {{ source('raw', 'raw_procurements_data') }}
