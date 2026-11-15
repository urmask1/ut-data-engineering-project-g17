with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('date_spine_start') ~ "' as date)",
        end_date="cast('" ~ var('date_spine_end') ~ "' as date)"
    ) }}
)
select
    to_char(date_day, 'YYYYMMDD')::int      as date_id,
    date_day::date                          as date,
    extract(year from date_day)::int        as year,
    extract(quarter from date_day)::int     as quarter
from spine
