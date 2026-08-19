{{ config(severity='error') }}

{#
  Singular test: bridge integrity between stg_policies and stg_claims.
  
  Business rule: every synthetic policy corresponds to exactly one FEMA claim
  and vice versa (1:1 bridge from Day 9 generator design).
  
  Test flags:
    - Policies with fema_claim_id that doesn't exist in stg_claims (orphan policy)
    - Claims with id that doesn't exist in stg_policies (orphan claim)
  
  Severity: error — bridge integrity is a core invariant. Any drift means
  the synthetic policy generator has diverged from the FEMA claim set.
  Fixable via regeneration.
#}

with policies as (
    select fema_claim_id from {{ ref('stg_policies') }}
    where fema_claim_id is not null
),

claims as (
    select fema_claim_id from {{ ref('stg_claims') }}
    where fema_claim_id is not null
),

-- policies with no matching claim 
orphan_policies as (
    select p.fema_claim_id, 'orphan_policy' as violation_type
    from policies p
    left join claims c on p.fema_claim_id = c.fema_claim_id
    where c.fema_claim_id is null
),

-- claims with no matching policy 
orphan_claims as (
    select cl.fema_claim_id, 'orphan_claim' as violation_type
    from claims cl
    left join policies pl on cl.fema_claim_id = pl.fema_claim_id
    where pl.fema_claim_id is null
),

all_violations as (
    select * from orphan_policies
    union all
    select * from orphan_claims
)

select * from all_violations