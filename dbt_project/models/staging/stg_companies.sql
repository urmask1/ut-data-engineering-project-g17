-- Üks rida registrikoodi kohta (raw_companies tabelis on topelt company_id, seda veergu ei kasutata)
select registration_code, company_name, registration_date, sector_code
from (
    select
        registration_code,
        trim(company_name)          as company_name, -- eemaldab tühikud
        case 
            when registration_date ~ '^\d{2}\.\d{2}\.\d{4}$' then to_date(registration_date, 'DD.MM.YYYY') 
            when registration_date ~ '^\d{4}-\d{2}-\d{2}$' then registration_date::date 
        end as registration_date,
        sector_code,
        row_number() over (
            partition by registration_code
            order by registration_date desc nulls last, company_id
        ) as rn
    from {{ source('raw', 'raw_companies') }}
    where registration_code is not null
) t
where rn = 1 -- Võtab igast registrikoodi grupist ainult rea, mis sai numbriks 1 ehk kõige uuema kirje
