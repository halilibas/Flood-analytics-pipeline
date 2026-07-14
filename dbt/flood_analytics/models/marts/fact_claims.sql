{{
  config(
    materialized='table',
    partition_by='loss_year'
  )
}}

with claims as (
    select * from {{ ref('stg_claims') }}
),

-- Bridge table: connects FEMA claim id to synthetic policy/customer/agent ids
policies_bridge as (
    select
        fema_claim_id,
        policy_number,
        customer_id,
        agent_id
    from {{ ref('stg_policies') }}
),

claims_bridged as (
    select
        c.*,
        p.policy_number,
        p.customer_id,
        p.agent_id
    from claims c
    left join policies_bridge p
      on c.fema_claim_id = p.fema_claim_id
),

-- Point-in-time join to SCD2 dim_policy 
with_policy_key as (
    select
        cb.*,
        dp.policy_key
    from claims_bridged cb
    left join {{ source('gold', 'dim_policy') }} dp
      on cb.policy_number = dp.policy_number
     and cb.date_of_loss >= dp.effective_date
     and cb.date_of_loss < coalesce(dp.expiration_date, date '9999-12-31')
),

-- Point-in-time join to SCD2 dim_customer
with_customer_key as (
    select
        wp.*,
        dc.customer_key
    from with_policy_key wp
    left join {{ source('gold', 'dim_customer') }} dc
      on wp.customer_id = dc.customer_id
     and wp.date_of_loss >= dc.effective_date
     and wp.date_of_loss < coalesce(dc.expiration_date, date '9999-12-31')
),

-- Simple SCD1 joins for agent and cat_event
with_agent_key as (
    select
        wc.*,
        da.agent_key
    from with_customer_key wc
    left join {{ ref('dim_agent') }} da
      on wc.agent_id = da.agent_id
),

with_cat_event_key as (
    select
        wa.*,
        dce.cat_event_key
    from with_agent_key wa
    left join {{ ref('dim_cat_event') }} dce
      on wa.flood_event_name = dce.event_name
),

-- Geography FK: recompute hash inline matching dim_geography
with_geography_hash as (
    select
        *,
        xxhash64(
            coalesce(state, ''),
            coalesce(county_fips, ''),
            coalesce(zip_code, ''),
            coalesce(census_tract, ''),
            coalesce(census_block_group_fips, ''),
            coalesce(cast(cast(latitude as decimal(6,1)) as string), ''),
            coalesce(cast(cast(longitude as decimal(6,1)) as string), ''),
            coalesce(nfip_community_name, ''),
            coalesce(nfip_community_number_at_rating, ''),
            coalesce(nfip_community_number_current, ''),
            coalesce(crs_class, '')
        ) as geography_key
    from with_cat_event_key
),

-- 4 role-playing date FKs against dim_date
with_date_keys as (
    select
        wg.*,
        d1.date_key as date_of_loss_key,
        d2.date_key as date_filed_key,
        d3.date_key as date_first_payment_key,
        d4.date_key as date_closed_key
    from with_geography_hash wg
    left join {{ ref('dim_date') }} d1 on wg.date_of_loss = d1.date
    left join {{ ref('dim_date') }} d2 on wg.date_filed = d2.date
    left join {{ ref('dim_date') }} d3 on wg.date_first_payment = d3.date
    left join {{ ref('dim_date') }} d4 on wg.date_closed = d4.date
),

-- Compute measures + surrogate + partition col + audit
final as (
    select
        -- Surrogate key
        xxhash64(fema_claim_id) as claim_key,

        -- Degenerate dim
        fema_claim_id,

        -- Partition column
        year(date_of_loss) as loss_year,

        -- Dim FKs
        policy_key,
        customer_key,
        agent_key,
        geography_key,
        cat_event_key,

        -- Role-playing date FKs
        date_of_loss_key,
        date_filed_key,
        date_first_payment_key,
        date_closed_key,

        -- Measures — paid
        building_claim_amount,
        contents_claim_amount,
        icc_claim_amount,
        coalesce(building_claim_amount, 0)
          + coalesce(contents_claim_amount, 0)
          + coalesce(icc_claim_amount, 0) as total_claim_amount,

        -- Measures — damage
        building_damage_amount,
        contents_damage_amount,

        -- Measures — coverage
        building_coverage_limit,
        contents_coverage_limit,
        icc_coverage_limit,

        -- Measures — event
        water_depth,

        -- Cycle time
        datediff(date_filed, date_of_loss) as days_loss_to_filed,
        datediff(date_first_payment, date_filed) as days_filed_to_first_payment,
        datediff(date_closed, date_filed) as days_filed_to_closed,

        -- Degenerate fact
        1 as claim_count
    from with_date_keys
)

select * from final