-- sector_id:  SCD2 (EMTAK jagu, ajalugu EMTA kvartaliandmetest)
-- field_id, company_name, registration_date: SCD1 (raw_companies, praegune väärtus)
with history as (
    select * from {{ ref('int_company_sector_history') }}
),
companies as (
    select * from {{ ref('stg_companies') }}
),
sectors as (
    select * from {{ ref('dim_sector') }}
),
fields as (
    select field_id from {{ ref('dim_field') }}
),

-- 1) ettevõtted, kellel on EMTA andmed -> versioonid ajaloost
with_history as (
    select
        {{ dbt_utils.generate_surrogate_key(['h.registration_code', 'h.valid_from']) }} as company_id,
        c.company_name,
        h.registration_code,
        c.registration_date,
        coalesce(s.sector_id, '-1')     as sector_id,
        coalesce(f.field_id, '-1')      as field_id,
        h.valid_from,
        h.valid_to,
        h.is_current
    from history h
    left join companies c using (registration_code)
    left join sectors s   on s.sector_name = h.sector_name
    left join fields f    on f.field_id = c.sector_code
),

-- 2) ettevõtted ilma EMTA andmeteta (nt hanke võitja ilma tegevuseta) -> üks versioon
without_history as (
    select
        {{ dbt_utils.generate_surrogate_key(['c.registration_code', "cast('1900-01-01' as date)"]) }} as company_id,
        c.company_name,
        c.registration_code,
        c.registration_date,
        '-1'                            as sector_id,
        coalesce(f.field_id, '-1')      as field_id,
        date '1900-01-01'               as valid_from,
        date '9999-12-31'               as valid_to,
        true                            as is_current
    from companies c
    left join fields f on f.field_id = c.sector_code
    where not exists (select 1 from history h where h.registration_code = c.registration_code)
)

select * from with_history
union all
select * from without_history
union all
-- 3) "Teadmata" liige faktiridadele, mille registrikoodi pole kusagil
select '-1', 'Teadmata ettevõte', null, null, '-1', '-1', date '1900-01-01', date '9999-12-31', true
