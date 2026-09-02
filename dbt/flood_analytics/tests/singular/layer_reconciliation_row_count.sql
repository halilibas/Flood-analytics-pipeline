{{ config(severity='error') }}

{#
  Singular test: layer reconciliation via row count, anchored on bronze.
  
  Business rule: silver.claims_clean and gold_dbt.fact_claims should
  contain the same number of rows as bronze.fema_claims_raw (2,721,780
  as of latest FEMA load).
  
  Bronze is source of truth: raw FEMA data as ingested. Silver applies
  cleaning rules including rejection of invalid rows. Gold materializes
  fact_claims from silver.
  
  Test anchors on bronze (SELECT COUNT FROM bronze source) rather than
  taking max(count) across layers. Anchoring on bronze:
    - Correctly identifies which layer diverged during an incident
    - Catches BOTH row loss (silver/gold < bronze) AND duplication
      (silver/gold > bronze), not just row loss
  
  Known limitation (documented for future work): this test hardcodes
  the assumption that silver never rejects a row. As of Day 26+, silver
  rejects 0 rows so the invariant holds. The day a rejection rule
  actually fires, this test fails by design. Correct long-term model is
  a quarantine table where the invariant becomes:
      bronze_count = silver_kept_count + silver_rejected_count
  Deferred to Week 6+.
  
  Test severity: error — row count mismatch means the pipeline is in
  a bad state and analytics will be wrong.
#}

with bronze_count as (
    select count(*) as expected_count
    from {{ source('bronze', 'fema_claims_raw') }}
),

layer_counts as (
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

violations as (
    select
        lc.layer,
        lc.row_count,
        bc.expected_count as bronze_row_count,
        (bc.expected_count - lc.row_count) as row_diff
    from layer_counts lc
    cross join bronze_count bc
    where lc.row_count != bc.expected_count
)

select * from violations