{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('silver', 'agents_clean') }}
),

renamed as (
    select
        agent_id,
        first_name as agent_first_name,
        last_name as agent_last_name,
        concat_ws(' ', first_name, last_name) as agent_full_name,
        email,
        phone,
        hire_date,
        agency_name,
        agency_state,
        commission_rate
    from source
)

select * from renamed