{{ config(severity='warn') }}

{#
  Singular test: paid amount vs FEMA-reported coverage limits.
  
  Business rule: total_claim_amount should not exceed the sum of
  FEMA-reported coverage limits (building + contents + ICC) by more
  than 10%. These are the coverage limits FEMA recorded on each claim,
  read directly from fact_claims (NOT from a point-in-time join to
  synthetic dim_policy — those come from a different source).
  
  Known counts (as of 2026-08-XX, at 2,721,780 fact_claims rows):
    - Strict  (threshold 1.00): 9,292 claims (0.341% of total)
    - Moderate (threshold 1.05): 8,369 claims (0.307% of total)
    - Current  (threshold 1.10): 5,743 claims (0.211% of total)
  
  Distribution:
    - 0–5% over cap:   ~923 claims
    - 5–10% over cap:  ~2,626 claims
    - >10% over cap:   5,743 claims
  
  Threshold 1.10 chosen because most violations are substantial (>10%),
  not marginal. Root cause per Day 36 investigation: nearly 100K FEMA
  source records have coverage values under $5K, which is below realistic
  NFIP policy amounts. Likely legacy claims with unit conversion issues,
  special coverage riders (ICC, contents-only), or payments-in-error that
  FEMA records without audit-correcting.
  
  Test severity: warn — flags as tracked anomaly, not blocking.
#}

with claim_coverage as (    -- renamed from fact_with_policy (was misleading)
    select
        f.claim_key,
        f.fema_claim_id,
        f.total_claim_amount,
        f.building_coverage_limit,
        f.contents_coverage_limit,
        f.icc_coverage_limit,
        coalesce(f.building_coverage_limit, 0)
          + coalesce(f.contents_coverage_limit, 0)
          + coalesce(f.icc_coverage_limit, 0) as total_coverage_limit
    from {{ ref('fact_claims') }} f
),
violations as (
    select
        claim_key,
        fema_claim_id,
        total_claim_amount,
        total_coverage_limit,
        round(total_claim_amount - total_coverage_limit, 2) as excess_amount,
        round((total_claim_amount / nullif(total_coverage_limit, 0) - 1) * 100, 2) as pct_over_cap
    from claim_coverage
    where total_coverage_limit > 0
      and total_claim_amount > total_coverage_limit * 1.10
)
select * from violations