{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('silver', 'customers_clean') }}
),

renamed as (
    select
        customer_id,
        first_name as customer_first_name,
        last_name as customer_last_name,
        concat_ws(' ', first_name, last_name) as customer_full_name,
        dob as date_of_birth,
        email,
        phone,
        address_line_1,
        address_state,
        occupation
    from source
)

select * from renamed