{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('silver', 'policies_clean') }}
),

renamed as (
    select
        -- Keys
        policy_number,
        fema_claim_id,        
        customer_id,
        agent_id,

        -- Coverage
        building_coverage,
        contents_coverage,
        deductible_amount,
        coverage_type,

        -- Economic
        annual_premium,

        -- Dates
        effective_date,
        expiration_date
    from source
)

select * from renamed
