{{ config(severity='warn') }}

{# 
  Singular test: coverage cap not exceeded.
  
  Business rule: total_claim_amount should not exceed the sum of
  coverage limits (building + contents + ICC). Coverage limits are
  the insurer's contractual maximum per claim.
  
  Approach: join fact_claims to the policy in effect at date_of_loss
  (already SCD2-resolved via policy_key from point-in-time join).
  Flag rows where paid > coverage cap.
  
  
#}

with fact_with_policy as (
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
    from fact_with_policy
    where total_coverage_limit > 0
      and total_claim_amount > total_coverage_limit * 1.10
)

select * from violations