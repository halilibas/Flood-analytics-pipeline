{% test scd2_no_overlapping_intervals(model, natural_key_column, version_column) %}

with ordered_versions as (
    select
        {{ natural_key_column }} as natural_key,
        {{ version_column }} as version,
        effective_date,
        expiration_date,
        is_current,
        lag(expiration_date) over (
            partition by {{ natural_key_column }}
            order by {{ version_column }}
        ) as prev_expiration_date
    from {{ model }}
),

interval_violations as (
    select
        natural_key,
        version,
        effective_date,
        prev_expiration_date,
        'interval_overlap' as violation_type
    from ordered_versions
    where prev_expiration_date is not null
      and effective_date < prev_expiration_date
),

current_flag_violations as (
    select
        natural_key,
        version,
        is_current,
        expiration_date,
        'current_flag_mismatch' as violation_type
    from ordered_versions
    where (is_current = true and expiration_date is not null)
       or (is_current = false and expiration_date is null)
),

all_violations as (
    select natural_key, version, violation_type from interval_violations
    union all
    select natural_key, version, violation_type from current_flag_violations
)

select * from all_violations

{% endtest %}
