# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - dim_geography (SCD Type 1)

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.claims_clean"
TARGET_TABLE = "gold.dim_geography"

PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)

# region rollup (US census divisons, simplified)
STATE_TO_REGION = {
    # northeast
    "CT": "Northeast", "ME": "Northeast", "MA": "Northeast", "NH": "Northeast",
    "NJ": "Northeast", "NY": "Northeast", "PA": "Northeast", "RI": "Northeast", "VT": "Northeast",
    # southeast
    "AL": "Southeast", "AR": "Southeast", "DE": "Southeast", "DC": "Southeast",
    "FL": "Southeast", "GA": "Southeast", "KY": "Southeast", "LA": "Southeast",
    "MD": "Southeast", "MS": "Southeast", "NC": "Southeast", "OK": "Southeast",
    "SC": "Southeast", "TN": "Southeast", "TX": "Southeast", "VA": "Southeast", "WV": "Southeast",
    # midwest
    "IL": "Midwest", "IN": "Midwest", "IA": "Midwest", "KS": "Midwest",
    "MI": "Midwest", "MN": "Midwest", "MO": "Midwest", "NE": "Midwest",
    "ND": "Midwest", "OH": "Midwest", "SD": "Midwest", "WI": "Midwest",
    # west
    "AK": "West", "AZ": "West", "CA": "West", "CO": "West", "HI": "West",
    "ID": "West", "MT": "West", "NV": "West", "NM": "West", "OR": "West",
    "UT": "West", "WA": "West", "WY": "West",
    # territories
    "PR": "Territories", "VI": "Territories", "GU": "Territories",
    "AS": "Territories", "MP": "Territories",
}

# costal
COASTAL_STATES = {
    "AK", "AL", "CA", "CT", "DE", "FL", "GA", "HI", "IL", "IN", "LA",
    "ME", "MD", "MA", "MI", "MN", "MS", "NH", "NJ", "NY", "NC", "OH",
    "OR", "PA", "RI", "SC", "TX", "VA", "WA", "WI", "PR", "VI", "GU",
    "AS", "MP", "DC",
}

print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Build dim_geography from silver

# COMMAND ----------

# derive unique geography combinations from claims

silver = spark.table(SOURCE_TABLE)

# build the base rollup - distinct geography per silver row
geo_raw = (
    silver
    .select(
        F.col("state"),
        F.col("countyCode").alias("county_fips"),
        F.col("reportedZipCode").alias("zip"),
        F.col("censusTract").alias("census_tract"),
        F.col("censusBlockGroupFips").alias("census_block_group_fips"),
        F.col("latitude").alias("lat"),
        F.col("longitude").alias("lng"),
        F.col("nfipCommunityName").alias("nfip_community_name"),
        F.col("nfipRatedCommunityNumber").alias("nfip_community_number_at_rating"),
        F.col("nfipCommunityNumberCurrent").alias("nfip_community_number_current"),
        F.col("crsClassificationCode").alias("crs_class"),
    )

    # drop rows with no useful geography at all
    .filter(F.col("state").isNotNull() | F.col("zip").isNotNull())
    .distinct()
)

print(f"Distinct geography rows: {geo_raw.count():,}")

# add derived attirbutes: region and is_costal
from pyspark.sql.functions import create_map, lit
from itertools import chain

region_map = F.create_map([lit(x) for x in chain(*STATE_TO_REGION.items())])
coastal_map = F.create_map([lit(x) for x in chain(*[(s, True) for s in COASTAL_STATES])])


dim_geography = (
    geo_raw
    .withColumn("region", F.coalesce(region_map[F.col("state")], F.lit("Unknown")))
    .withColumn("is_coastal", F.coalesce(coastal_map[F.col("state")], F.lit(False)))
    # surrogate key: hash of the identifying columns
    .withColumn(
    "geography_key",
    F.xxhash64(
        F.coalesce(F.col("state"), F.lit("")),
        F.coalesce(F.col("county_fips"), F.lit("")),
        F.coalesce(F.col("zip"), F.lit("")),
        F.coalesce(F.col("census_tract"), F.lit("")),
        F.coalesce(F.col("census_block_group_fips"), F.lit("")),
        F.coalesce(F.col("lat").cast("decimal(6,1)").cast("string"), F.lit("")),
        F.coalesce(F.col("lng").cast("decimal(6,1)").cast("string"), F.lit("")),
        F.coalesce(F.col("nfip_community_name"), F.lit("")),
        F.coalesce(F.col("nfip_community_number_at_rating"), F.lit("")),
        F.coalesce(F.col("nfip_community_number_current"), F.lit("")),
        F.coalesce(F.col("crs_class"), F.lit("")),
    )
)

    .withColumn("_dim_built_at", F.lit(INGESTED_AT).cast("timestamp"))
    .withColumn("_dim_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
    .select(
        "geography_key",
        "state", "region", "is_coastal",
        "county_fips", "zip",
        "census_tract", "census_block_group_fips",
        "lat", "lng",
        "nfip_community_name", "nfip_community_number_at_rating", "nfip_community_number_current",
        "crs_class",
        "_dim_built_at", "_dim_pipeline_run_id",
    )
)

# verify surrogate uniqueness
n_rows = dim_geography.count()
n_distinct = dim_geography.select("geography_key").distinct().count()
print(f"Total rows: {n_rows:,}")
print(f"Distinct geography_key: {n_distinct:,}")







# COMMAND ----------

# MAGIC %md
# MAGIC ### Write and Verify

# COMMAND ----------

(
    dim_geography.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Wrote {TARGET_TABLE}")

# sample rows

spark.sql(f"""
    SELECT state, region, is_coastal, county_fips, zip,
        nfip_community_name, crs_class
    FROM {TARGET_TABLE}
    WHERE state IS NOT NULL
    ORDER BY state          
    LIMIT 10       
""").display()

# region distribution
spark.sql(f"""
    SELECT region, COUNT(*) AS n
    FROM {TARGET_TABLE}
    GROUP BY region
    ORDER BY n DESC          
""").display()

# coastal vs non-coastal
spark.sql(f"""
    SELECT is_coastal, COUNT(*) AS n
    FROM {TARGET_TABLE}          
    GROUP BY is_coastal     
""").display()

