{{
  config(
    materialized='table'
  )
}}

WITH SOURCE AS (
    SELECT DISTINCT
        state,
        county_fips,
        zip_code AS zip,
        census_tract,
        census_block_group_fips,
        latitude AS lat,
        longitude AS lng,
        nfip_community_name,
        nfip_community_number_at_rating,
        nfip_community_number_current,
        crs_clASs
    FROM {{ ref('stg_claims') }}
    WHERE state IS NOT NULL OR zip_code IS NOT NULL
),

with_derived AS (
    SELECT
        *,
        CASE state
            WHEN 'CT' THEN 'Northeast' WHEN 'ME' THEN 'Northeast' WHEN 'MA' THEN 'Northeast'
            WHEN 'NH' THEN 'Northeast' WHEN 'NJ' THEN 'Northeast' WHEN 'NY' THEN 'Northeast'
            WHEN 'PA' THEN 'Northeast' WHEN 'RI' THEN 'Northeast' WHEN 'VT' THEN 'Northeast'

            WHEN 'AL' THEN 'Southeast' WHEN 'AR' THEN 'Southeast' WHEN 'DE' THEN 'Southeast'
            WHEN 'DC' THEN 'Southeast' WHEN 'FL' THEN 'Southeast' WHEN 'GA' THEN 'Southeast'
            WHEN 'KY' THEN 'Southeast' WHEN 'LA' THEN 'Southeast' WHEN 'MD' THEN 'Southeast'
            WHEN 'MS' THEN 'Southeast' WHEN 'NC' THEN 'Southeast' WHEN 'OK' THEN 'Southeast'
            WHEN 'SC' THEN 'Southeast' WHEN 'TN' THEN 'Southeast' WHEN 'TX' THEN 'Southeast'
            WHEN 'VA' THEN 'Southeast' WHEN 'WV' THEN 'Southeast'

            WHEN 'IL' THEN 'Midwest' WHEN 'IN' THEN 'Midwest' WHEN 'IA' THEN 'Midwest'
            WHEN 'KS' THEN 'Midwest' WHEN 'MI' THEN 'Midwest' WHEN 'MN' THEN 'Midwest'
            WHEN 'MO' THEN 'Midwest' WHEN 'NE' THEN 'Midwest' WHEN 'ND' THEN 'Midwest'
            WHEN 'OH' THEN 'Midwest' WHEN 'SD' THEN 'Midwest' WHEN 'WI' THEN 'Midwest'

            WHEN 'AK' THEN 'West' WHEN 'AZ' THEN 'West' WHEN 'CA' THEN 'West'
            WHEN 'CO' THEN 'West' WHEN 'HI' THEN 'West' WHEN 'ID' THEN 'West'
            WHEN 'MT' THEN 'West' WHEN 'NV' THEN 'West' WHEN 'NM' THEN 'West'
            WHEN 'OR' THEN 'West' WHEN 'UT' THEN 'West' WHEN 'WA' THEN 'West'
            WHEN 'WY' THEN 'West'

            WHEN 'PR' THEN 'Territories' WHEN 'VI' THEN 'Territories' WHEN 'GU' THEN 'Territories'
            WHEN 'AS' THEN 'Territories' WHEN 'MP' THEN 'Territories'

            ELSE 'Unknown'
        END AS region,

        state IN (
            'AK', 'AL', 'CA', 'CT', 'DE', 'FL', 'GA', 'HI', 'IL', 'IN', 'LA',
            'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'NH', 'NJ', 'NY', 'NC', 'OH',
            'OR', 'PA', 'RI', 'SC', 'TX', 'VA', 'WA', 'WI', 'PR', 'VI', 'GU',
            'AS', 'MP', 'DC'
        ) AS is_coAStal
    FROM SOURCE
),

WITH_key AS (
    SELECT
        xxhASh64(
            coalesce(state, ''),
            coalesce(county_fips, ''),
            coalesce(zip, ''),
            coalesce(census_tract, ''),
            coalesce(census_block_group_fips, ''),
            coalesce(cASt(cASt(lat AS decimal(6,1)) AS string), ''),
            coalesce(cASt(cASt(lng AS decimal(6,1)) AS string), ''),
            coalesce(nfip_community_name, ''),
            coalesce(nfip_community_number_at_rating, ''),
            coalesce(nfip_community_number_current, ''),
            coalesce(crs_clASs, '')
        ) AS geography_key,
        state,
        region,
        is_coAStal,
        county_fips,
        zip,
        census_tract,
        census_block_group_fips,
        lat,
        lng,
        nfip_community_name,
        nfip_community_number_at_rating,
        nfip_community_number_current,
        crs_clASs
    FROM WITH_derived
),

WITH_sentinel AS (
    SELECT * FROM WITH_key
    UNION ALL
    SELECT
        xxhASh64('', '', '', '', '', '', '', '', '', '', '') AS geography_key,
        null AS state,
        'Unknown' AS region,
        false AS is_coAStal,
        null AS county_fips,
        null AS zip,
        null AS census_tract,
        null AS census_block_group_fips,
        cASt(null AS double) AS lat,
        cASt(null AS double) AS lng,
        '[UNKNOWN GEOGRAPHY]' AS nfip_community_name,
        null AS nfip_community_number_at_rating,
        null AS nfip_community_number_current,
        null AS crs_clASs
)

SELECT * FROM WITH_sentinel