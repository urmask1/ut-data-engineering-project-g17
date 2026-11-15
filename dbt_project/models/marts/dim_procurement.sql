select
    contract_id     as procurement_id,
    contract_id     as procurement_contract_id,
    eu_funded
from {{ ref('stg_procurements') }}
