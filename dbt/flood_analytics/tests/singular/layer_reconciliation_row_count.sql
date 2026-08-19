{{ config(severity='error') }}

{#
  Singular test: layer reconciliation via row count.
  
  
  Severity: error — a row count mismatch means the pipeline is in a bad
  state and analytics will be wrong.
#}

with layer_counts as (
    select
        'bronze.fema_claims_raw' as layer,
        count(*) as row_count
    from {{ source('bronze', 'fema_claims_raw') }}

    union all

    select
        'silver.claims_clean' as layer,
        count(*) as row_count
    from {{ source('silver', 'claims_clean') }}

    union all

    select
        'gold_dbt.fact_claims' as layer,
        count(*) as row_count
    from {{ ref('fact_claims') }}
),

max_count as (
    select max(row_count) as expected_count from layer_counts
),

-- Flag any layer where row count differs from the max (should all be equal)
violations as (
    select
        lc.layer,
        lc.row_count,
        mc.expected_count,
        (mc.expected_count - lc.row_count) as row_diff
    from layer_counts lc
    cross join max_count mc
    where lc.row_count != mc.expected_count
)

select * from violations