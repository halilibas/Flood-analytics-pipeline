# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - fact_claims v1

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType

SOURCE_CLAIMS = "silver.claims_clean"
DIM_DATE = "gold.dim_date"
DIM_POLICY = "gold.dim_policy"
DIM_CUSTOMER = "gold.dim_customer"
DIM_AGENT = "gold.dim_agent"
DIM_GEOGRAPHY = "gold.dim_geography"
DIM_CAT_EVENT = "gold.dim_cat_event"
SILVER_POLICIES = "silver.policies_clean"  

TARGET_TABLE = "gold.fact_claims"

PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)

print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Source and Inspect

# COMMAND ----------

claims = spark.table(SOURCE_CLAIMS)
print(f"silver.claims_clean: {claims.count():,} rows")


claims.select(
    "id", "dateOfLoss", "date_filed", "date_first_payment", "date_closed",
    "state", "countyCode", "reportedZipCode", "censusTract", "censusBlockGroupFips",
    "latitude", "longitude", "nfipCommunityName",
    "nfipRatedCommunityNumber", "nfipCommunityNumberCurrent", "crsClassificationCode",
    "floodEvent",
    "amountPaidOnBuildingClaim", "amountPaidOnContentsClaim",
    "amountPaidOnIncreasedCostOfComplianceClaim",
    "totalBuildingInsuranceCoverage", "totalContentsInsuranceCoverage", "iccCoverage",
    "buildingDamageAmount", "contentsDamageAmount",
    "waterDepth",
).limit(2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolve policy_key and consumer_key (point-in-time via SCD2)

# COMMAND ----------

# bridge from FEMA claim id to policy_number / customer_id via silver.policies_clean

bridge = spark.table(SILVER_POLICIES).select(
    F.col("fema_claim_id"),
    F.col("policy_number"),
    F.col("customer_id"),
    F.col("agent_id"),
)


claims_with_bridge = claims.join(
    bridge,
    claims["id"] == bridge["fema_claim_id"],
    how="left",
).drop("fema_claim_id")

print(f"After bridge join: {claims_with_bridge.count():,}")


# COMMAND ----------

# point-in-time join to dim_policy on dateOfLoss
dim_policy = spark.table(DIM_POLICY).select(
    F.col("policy_key"),
    F.col("policy_number").alias("dp_policy_number"),
    F.col("effective_date").alias("dp_effective_date"),
    F.col("expiration_date").alias("dp_expiration_date"),
)

claims_with_policy = claims_with_bridge.join(
    dim_policy,
    (claims_with_bridge["policy_number"] == dim_policy["dp_policy_number"])
    & (claims_with_bridge["dateOfLoss"] >= dim_policy["dp_effective_date"])
    & (
        (dim_policy["dp_expiration_date"].isNull())
        | (claims_with_bridge["dateOfLoss"] < dim_policy["dp_expiration_date"])
    ),
    how="left",
).drop("dp_policy_number", "dp_effective_date", "dp_expiration_date")

n_after_policy = claims_with_policy.count()
print(f"After policy point-in-time join: {n_after_policy:,}")
assert n_after_policy == 2_721_780, "Row count changed after policy join"

n_policy_null = claims_with_policy.filter(F.col("policy_key").isNull()).count()
print(f"Claims with null policy_key: {n_policy_null:,}")

# COMMAND ----------

# point-in-time join to dim_customer on dateOfLoss
dim_customer = spark.table(DIM_CUSTOMER).select(
    F.col("customer_key"),
    F.col("customer_id").alias("dc_customer_id"),
    F.col("effective_date").alias("dc_effective_date"),
    F.col("expiration_date").alias("dc_expiration_date"),
)

claims_with_customer = claims_with_policy.join(
    dim_customer,
    (claims_with_policy["customer_id"] == dim_customer["dc_customer_id"])
    & (claims_with_policy["dateOfLoss"] >= dim_customer["dc_effective_date"])
    & (
        (dim_customer["dc_expiration_date"].isNull())
        | (claims_with_policy["dateOfLoss"] < dim_customer["dc_expiration_date"])
    ),
    how="left",
).drop("dc_customer_id", "dc_effective_date", "dc_expiration_date")

n_after_customer = claims_with_customer.count()
print(f"After customer point-in-time join: {n_after_customer:,}")
assert n_after_customer == 2_721_780, "Row count changed after customer join"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolve Simple FKs

# COMMAND ----------

# agent — simple join on natural key
dim_agent = spark.table(DIM_AGENT).select(
    F.col("agent_key"),
    F.col("agent_id").alias("da_agent_id"),
)

claims_with_agent = claims_with_customer.join(
    dim_agent,
    claims_with_customer["agent_id"] == dim_agent["da_agent_id"],
    how="left",
).drop("da_agent_id")

n_after_agent = claims_with_agent.count()
print(f"After agent join: {n_after_agent:,}")
assert n_after_agent == 2_721_780

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cat_event Join

# COMMAND ----------

# cat_event — LEFT join, nullable
dim_cat_event = spark.table(DIM_CAT_EVENT).select(
    F.col("cat_event_key"),
    F.col("event_name").alias("dce_event_name"),
)

claims_with_cat = claims_with_agent.join(
    dim_cat_event,
    claims_with_agent["floodEvent"] == dim_cat_event["dce_event_name"],
    how="left",
).drop("dce_event_name")

n_after_cat = claims_with_cat.count()
print(f"After cat_event join: {n_after_cat:,}")
assert n_after_cat == 2_721_780

spark.sql("SELECT 1").collect()
cat_stats = claims_with_cat.selectExpr(
    "SUM(CASE WHEN cat_event_key IS NULL THEN 1 ELSE 0 END) AS n_null",
    "SUM(CASE WHEN cat_event_key IS NOT NULL THEN 1 ELSE 0 END) AS n_present"
).collect()[0]
print(f"cat_event_key NULL: {cat_stats['n_null']:,} | present: {cat_stats['n_present']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Geography FK

# COMMAND ----------

# compute geography_key on the fly from claim columns 
claims_with_geo = claims_with_cat.withColumn(
    "geography_key",
    F.xxhash64(
        F.coalesce(F.col("state"), F.lit("")),
        F.coalesce(F.col("countyCode"), F.lit("")),
        F.coalesce(F.col("reportedZipCode"), F.lit("")),
        F.coalesce(F.col("censusTract"), F.lit("")),
        F.coalesce(F.col("censusBlockGroupFips"), F.lit("")),
        F.coalesce(F.col("latitude").cast("decimal(6,1)").cast("string"), F.lit("")),
        F.coalesce(F.col("longitude").cast("decimal(6,1)").cast("string"), F.lit("")),
        F.coalesce(F.col("nfipCommunityName"), F.lit("")),
        F.coalesce(F.col("nfipRatedCommunityNumber"), F.lit("")),
        F.coalesce(F.col("nfipCommunityNumberCurrent"), F.lit("")),
        F.coalesce(F.col("crsClassificationCode"), F.lit("")),
    )
)


dim_geo_keys = spark.table(DIM_GEOGRAPHY).select("geography_key")
unresolved = claims_with_geo.join(
    dim_geo_keys, on="geography_key", how="left_anti"
).count()

print(f"Claims with unresolved geography_key: {unresolved:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolve date FKs

# COMMAND ----------

dim_date = spark.table(DIM_DATE).select(
    F.col("date_key"),
    F.col("date"),
)

# date_of_loss_key
step1 = claims_with_geo.join(
    dim_date.select(F.col("date").alias("_d1"), F.col("date_key").alias("date_of_loss_key")),
    claims_with_geo["dateOfLoss"] == F.col("_d1"),
    how="left",
).drop("_d1")

# date_filed_key
step2 = step1.join(
    dim_date.select(F.col("date").alias("_d2"), F.col("date_key").alias("date_filed_key")),
    step1["date_filed"] == F.col("_d2"),
    how="left",
).drop("_d2")

# date_first_payment_key (nullable — 21% of claims)
step3 = step2.join(
    dim_date.select(F.col("date").alias("_d3"), F.col("date_key").alias("date_first_payment_key")),
    step2["date_first_payment"] == F.col("_d3"),
    how="left",
).drop("_d3")

# date_closed_key
step4 = step3.join(
    dim_date.select(F.col("date").alias("_d4"), F.col("date_key").alias("date_closed_key")),
    step3["date_closed"] == F.col("_d4"),
    how="left",
).drop("_d4")

n_after_dates = step4.count()
print(f"After 4 date FKs: {n_after_dates:,}")
assert n_after_dates == 2_721_780

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compute Measures and Select Final Column

# COMMAND ----------

fact = (
    step4
    # Surrogate key
    .withColumn("claim_key", F.xxhash64(F.col("id")))
 
    .withColumnRenamed("id", "fema_claim_id")
  
    .withColumn("claim_count", F.lit(1).cast(IntegerType()))

    .withColumn(
        "total_claim_amount",
        F.coalesce(F.col("amountPaidOnBuildingClaim"), F.lit(0))
        + F.coalesce(F.col("amountPaidOnContentsClaim"), F.lit(0))
        + F.coalesce(F.col("amountPaidOnIncreasedCostOfComplianceClaim"), F.lit(0))
    )
    # cycle time measures (from lifecycle dates)
    .withColumn("days_loss_to_filed", F.datediff(F.col("date_filed"), F.col("dateOfLoss")))
    .withColumn("days_filed_to_first_payment", F.datediff(F.col("date_first_payment"), F.col("date_filed")))
    .withColumn("days_filed_to_closed", F.datediff(F.col("date_closed"), F.col("date_filed")))
 
    .withColumn("loss_year", F.year(F.col("dateOfLoss")))
    
    .withColumn("_fact_built_at", F.lit(INGESTED_AT).cast("timestamp"))
    .withColumn("_fact_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
)

# final column selection
fact_final = fact.select(
    # keys and degenerate dim
    "claim_key",
    "fema_claim_id",
    "loss_year",  

    # dimension FKs
    "policy_key",
    "customer_key",
    "agent_key",
    "geography_key",
    "cat_event_key",

    # role-playing date FKs
    "date_of_loss_key",
    "date_filed_key",
    "date_first_payment_key",
    "date_closed_key",

    # measures — paid
    F.col("amountPaidOnBuildingClaim").alias("building_claim_amount"),
    F.col("amountPaidOnContentsClaim").alias("contents_claim_amount"),
    F.col("amountPaidOnIncreasedCostOfComplianceClaim").alias("icc_claim_amount"),
    "total_claim_amount",

    # measures — damage
    F.col("buildingDamageAmount").alias("building_damage_amount"),
    F.col("contentsDamageAmount").alias("contents_damage_amount"),

    # measures — coverage
    F.col("totalBuildingInsuranceCoverage").alias("building_coverage_limit"),
    F.col("totalContentsInsuranceCoverage").alias("contents_coverage_limit"),
    F.col("iccCoverage").alias("icc_coverage_limit"),

    # measures — event
    F.col("waterDepth").alias("water_depth"),

    # cycle time
    "days_loss_to_filed",
    "days_filed_to_first_payment",
    "days_filed_to_closed",

    # Degenerate fact
    "claim_count",

    # Audit
    "_fact_built_at",
    "_fact_pipeline_run_id",
)

print(f"fact_claims v1: {fact_final.count():,} rows × {len(fact_final.columns)} cols")
fact_final.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write with Partioning

# COMMAND ----------

(
    fact_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("loss_year")
    .saveAsTable(TARGET_TABLE)
)

print(f"Wrote {TARGET_TABLE} partitioned by loss_year")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify FK Integrity and Grain

# COMMAND ----------

# row count assertion
n_fact = spark.table(TARGET_TABLE).count()
print(f"fact_claims v1 rows: {n_fact:,}  (expected 2,721,780)")
assert n_fact == 2_721_780

# claim_key uniqueness
n_distinct_claim = spark.sql(f"SELECT COUNT(DISTINCT claim_key) FROM {TARGET_TABLE}").collect()[0][0]
print(f"Distinct claim_key: {n_distinct_claim:,}")
assert n_distinct_claim == n_fact, "claim_key not unique"

# NULL FK summary — some are expected (cat_event, date_first_payment), others not
spark.sql(f"""
    SELECT
        SUM(CASE WHEN policy_key IS NULL THEN 1 ELSE 0 END) AS null_policy,
        SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS null_customer,
        SUM(CASE WHEN agent_key IS NULL THEN 1 ELSE 0 END) AS null_agent,
        SUM(CASE WHEN geography_key IS NULL THEN 1 ELSE 0 END) AS null_geography,
        SUM(CASE WHEN cat_event_key IS NULL THEN 1 ELSE 0 END) AS null_cat_event,
        SUM(CASE WHEN date_of_loss_key IS NULL THEN 1 ELSE 0 END) AS null_date_of_loss,
        SUM(CASE WHEN date_filed_key IS NULL THEN 1 ELSE 0 END) AS null_date_filed,
        SUM(CASE WHEN date_first_payment_key IS NULL THEN 1 ELSE 0 END) AS null_date_first_pay,
        SUM(CASE WHEN date_closed_key IS NULL THEN 1 ELSE 0 END) AS null_date_closed
    FROM {TARGET_TABLE}
""").display()

# COMMAND ----------

# The "join every dim" query — proves the star schema works end-to-end
spark.sql(f"""
    SELECT
        d.year,
        c.event_type,
        g.state,
        g.is_coastal,
        COUNT(*) AS n_claims,
        ROUND(AVG(f.days_filed_to_closed), 0) AS avg_days_to_close,
        ROUND(AVG(f.total_claim_amount), 0) AS avg_severity,
        ROUND(SUM(f.total_claim_amount) / 1e9, 2) AS total_paid_billions
    FROM {TARGET_TABLE} f
    JOIN {DIM_DATE} d           ON f.date_of_loss_key = d.date_key
    LEFT JOIN {DIM_CAT_EVENT} c ON f.cat_event_key = c.cat_event_key
    JOIN {DIM_GEOGRAPHY} g      ON f.geography_key = g.geography_key
    WHERE d.year IN (2005, 2012, 2017, 2022)
      AND c.event_type = 'Hurricane'
    GROUP BY d.year, c.event_type, g.state, g.is_coastal
    ORDER BY total_paid_billions DESC
    LIMIT 15
""").display()