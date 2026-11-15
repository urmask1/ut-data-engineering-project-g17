-- ettevõtte sektori (EMTAK jagu) ajalugu EMTA kvartaliandmetest
-- Uus versioon tekib siis, kui sektor kvartalite vahel muutub.
-- Tühja sektoriga kvartalid ei loo uut versiooni (kehtima jääb viimane teadaolev sektor);
-- 'Teadmata' versioon tekib ainult ettevõttele, kelle sektor pole üheski kvartalis teada.
with src as (
    select registration_code, quarter_start, sector_name
    from {{ ref('stg_emta') }}
),
known as (
    select * from src where sector_name <> 'Teadmata'
    union all
    select * from src u
    where u.sector_name = 'Teadmata'
      and not exists (select 1 from src k
                      where k.registration_code = u.registration_code
                        and k.sector_name <> 'Teadmata')
),
flagged as (
    select
        registration_code,
        quarter_start,
        sector_name,
        case when sector_name is distinct from
                  lag(sector_name) over (partition by registration_code order by quarter_start)
             then 1 else 0 end as is_change
    from known
),
grouped as (
    select *,
        sum(is_change) over (partition by registration_code order by quarter_start) as version_no
    from flagged
),
versions as (
    select registration_code, sector_name, version_no, min(quarter_start) as change_date
    from grouped
    group by registration_code, sector_name, version_no
)
select
    registration_code,
    sector_name,
    version_no,
    -- esimene versioon kehtib "algusest", et ka varasemad hanked leiaksid ettevõtte
    case when version_no = 1 then date '1900-01-01' else change_date end    as valid_from,
    coalesce((lead(change_date) over w - 1)::date, date '9999-12-31')       as valid_to,
    lead(change_date) over w is null                                         as is_current
from versions
window w as (partition by registration_code order by version_no)
