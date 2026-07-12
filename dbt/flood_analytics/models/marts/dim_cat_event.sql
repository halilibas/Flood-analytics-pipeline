{{
    config(
        materialized='table'
    )
}}

WITH SOURCE AS (
    SELECT DISTINCT flood_event_name AS event_name
    FROM {{ ref('stg_claims') }}
    WHERE flood_event_name IS NOT NULL
),

categorized as (
    SELECT
        event_name,
        CASE
            WHEN event_name LIKE 'Hurricane%' THEN 'Hurricane'
            WHEN event_name LIKE 'Tropical Storm%' THEN 'Tropical Storm'
            WHEN event_name IN ('Flooding', 'Not a named storm') THEN 'UNNAMED'
            ELSE 'Other'
        END AS event_type
    FROM source
),

final AS (
    SELECT
        xxhash64(event_name) AS cat_event_key,
        event_name,
        event_type,
        event_type in ('Hurricane', 'Tropical Storm') AS is_named_storm
    FROM categorized
)

SELECT * FROM final

