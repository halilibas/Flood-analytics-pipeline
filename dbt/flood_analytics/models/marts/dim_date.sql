{{
  config(
    materialized='table'
  )
}}

with date_spine as (
    select explode(sequence(
        to_date('1970-01-01'),
        to_date('2030-12-31'),
        interval 1 day
    )) as date
),

enriched as (
    select
        cast(date_format(date, 'yyyyMMdd') as int) as date_key,
        date,
        year(date) as year,
        quarter(date) as quarter,
        concat('Q', quarter(date), ' ', year(date)) as quarter_year,
        month(date) as month,
        date_format(date, 'MMMM') as month_name,
        date_format(date, 'MMM') as month_short,
        date_format(date, 'yyyy-MM') as year_month,
        day(date) as day,
        dayofyear(date) as day_of_year,
        dayofweek(date) as day_of_week,
        date_format(date, 'EEEE') as day_name,
        date_format(date, 'EEE') as day_name_short,
        weekofyear(date) as week_of_year,
        dayofweek(date) in (1, 7) as is_weekend,
        (month(date) >= 6 and month(date) <= 11) as hurricane_season_flag
    from date_spine
)

select * from enriched