-- Grain: üks rida hankelepingu kohta (ühispakkumisel on lepingul faktitabelis mitu rida)
select distinct
    procurement_contract_id     as procurement_id,
    procurement_contract_id,
    eu_funded,
    is_framework_agreement
from {{ ref('stg_procurements') }}
