-- models/marts/dimensions/dim_date.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Date dimension: one row per calendar day.
-- Pre-populated for 10 years (2020–2030).
-- Surrogate key: YYYYMMDD integer (e.g., 20240115 for Jan 15 2024)
-- ─────────────────────────────────────────────────────────────────────────────

{{
    config(
        materialized = 'table',
        tags         = ['dimensions', 'gold', 'static'],
        description  = 'Calendar date dimension. Pre-populated 2020-2030.'
    )
}}

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart   = "day",
        start_date = "cast('2020-01-01' as date)",
        end_date   = "cast('2030-12-31' as date)"
    ) }}

),

final as (

    select
        -- Surrogate key (YYYYMMDD integer — fast joins, human-readable)
        cast(strftime(date_day, '%Y%m%d') as integer)   as date_sk,

        -- Full date
        date_day                                         as full_date,

        -- Year
        extract(year  from date_day)::integer            as year,

        -- Quarter
        extract(quarter from date_day)::integer          as quarter,
        'Q' || extract(quarter from date_day)::varchar   as quarter_name,

        -- Month
        extract(month from date_day)::integer            as month,
        strftime(date_day, '%B')                         as month_name,
        strftime(date_day, '%b')                         as month_name_short,

        -- Week
        extract(week from date_day)::integer             as week_of_year,

        -- Day
        extract(day  from date_day)::integer             as day_of_month,
        extract(dow  from date_day)::integer             as day_of_week,    -- 0=Sunday
        strftime(date_day, '%A')                         as day_name,
        strftime(date_day, '%a')                         as day_name_short,

        -- Flags
        case
            when extract(dow from date_day) in (0, 6) then true
            else false
        end                                              as is_weekend,

        -- Is current day / week / month / year
        date_day = current_date                          as is_today,
        extract(year  from date_day) = extract(year  from current_date)
            and extract(week from date_day) = extract(week from current_date)
                                                         as is_current_week,
        extract(year  from date_day) = extract(year  from current_date)
            and extract(month from date_day) = extract(month from current_date)
                                                         as is_current_month,
        extract(year  from date_day) = extract(year  from current_date)
                                                         as is_current_year,

        -- Days from today (negative = past, positive = future)
        cast(date_day - current_date as integer)         as days_from_today,

        -- YYYYMM for month-level aggregations
        cast(strftime(date_day, '%Y%m') as integer)      as year_month_key

    from date_spine

)

select * from final
