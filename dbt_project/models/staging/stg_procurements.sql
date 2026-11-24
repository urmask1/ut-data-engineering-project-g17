-- Grain: üks rida lepingu ja Eesti võitja kohta (record_id)
-- Alles jäävad ainult Eesti ettevõtted (8-kohaline registrikood). Riiki on vaja kontrollida, sest ka
-- välismaiste võitjate koodid võivad olla 8-kohalised numbrid. Eesti 11-kohalised koodid on isikukoodid.
-- Ühispakkumise summa jagatakse võrdselt kõigi võitjate vahel (ka välja filtreeritute), et summat ei loetaks topelt.
select
    record_id,
    notice_id || '/' || contract_id                 as procurement_contract_id, -- contract_id (nt CON-0001) on unikaalne ainult teate sees
    contract_issued_at                              as award_date,
    case when company_registration_code ~ '^[0-9]{8}$'
         then company_registration_code::int
    end                                             as registration_code,
    (contract_award_amount / winners_in_contract)::numeric(18, 2) as awarded_contract_amount, -- NULL = summat pole avaldatud
    case lower(trim(eu_funded))
        when 'no-eu-funds' then false
        when 'eu-funds'    then true
    end                                             as eu_funded,
    framework_agreement <> 'none'                   as is_framework_agreement -- NULL = tunnust pole avaldatud
from {{ source('raw', 'raw_procurements_data') }}
where company_country = 'EST'
  and company_registration_code ~ '^[0-9]{8}$'
