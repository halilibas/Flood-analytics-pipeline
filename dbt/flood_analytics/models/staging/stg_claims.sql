{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('silver', 'claims_clean') }}
),

renamed as (
    select
        -- Keys
        id as fema_claim_id,

        -- Dates (real + synthesized)
        dateOfLoss as date_of_loss,
        date_filed,
        date_first_payment,
        date_closed,

        -- Location
        state,
        countyCode as county_fips,
        reportedZipCode as zip_code,
        censusTract as census_tract,
        censusBlockGroupFips as census_block_group_fips,
        latitude,
        longitude,
        nfipCommunityName as nfip_community_name,
        nfipRatedCommunityNumber as nfip_community_number_at_rating,
        nfipCommunityNumberCurrent as nfip_community_number_current,
        crsClassificationCode as crs_class,

        -- Event
        floodEvent as flood_event_name,

        -- Coverage
        totalBuildingInsuranceCoverage as building_coverage_limit,
        totalContentsInsuranceCoverage as contents_coverage_limit,
        iccCoverage as icc_coverage_limit,

        -- Damage
        buildingDamageAmount as building_damage_amount,
        contentsDamageAmount as contents_damage_amount,

        -- Paid
        amountPaidOnBuildingClaim as building_claim_amount,
        amountPaidOnContentsClaim as contents_claim_amount,
        amountPaidOnIncreasedCostOfComplianceClaim as icc_claim_amount,

        -- Event characteristic
        waterDepth as water_depth,

        -- Property attributes (raw — pass through)
        occupancyType as occupancy_type_code,
        causeOfDamage as cause_of_damage_code,
        buildingDescriptionCode as building_description_code,
        ratedFloodZone as flood_zone_code
    from source
)

select * from renamed