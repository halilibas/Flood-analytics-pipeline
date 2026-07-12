{{
  config(
    materialized='table'
  )
}}

with source as (
    select * from {{ ref('stg_agents') }}
),

with_key as (
    select
        xxhash64(agent_id) as agent_key,
        agent_id,
        agent_first_name,
        agent_last_name,
        agent_full_name,
        agency_name,
        agency_state,
        email,
        phone,
        hire_date,
        commission_rate
    from source
)

select * from with_key