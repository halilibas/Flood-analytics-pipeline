# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Enrich Claims with Policy / Customer / Agent

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4
from pyspark.sql import functions as F

PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Silvers

# COMMAND ----------

claims = spark.table("silver.claims_clean")
policies = spark.table("silver.policies_clean")
customers = spark.table("silver.customers_clean")
agents = spark.table("silver.agents_clean")

print(f"Claims: {claims.count():,}")
print(f"Policies: {policies.count():,}")
print(f"Customers: {customers.count():,}")
print(f"Agents: {agents.count():,}")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare Column Subsets

# COMMAND ----------

claims_for_join = claims

# From policies: rename to avoid duplicate FEMA columns
policies_for_join = (
    policies.select(
        F.col("fema_claim_id"),
        F.col("policy_number"),
        F.col("customer_id"),
        F.col("agent_id"),
        F.col("effective_date").alias("policy_effective_date"),
        F.col("expiration_date").alias("policy_expiration_date"),
        F.col("building_coverage").alias("policy_building_coverage"),
        F.col("contents_coverage").alias("policy_contents_coverage"),
        F.col("deductible_amount").alias("policy_deductible"),
        F.col("annual_premium").alias("policy_annual_premium"),
        F.col("coverage_type").alias("policy_coverage_type"),

    )
)

# From customers: select essentials. prefix with customer _*
customers_for_join = (
    customers.select(
        F.col("customer_id"),
        F.col("fema_claim_id"),
        F.col("first_name").alias("customer_first_name"),
        F.col("last_name").alias("customer_last_name"),
        F.col("dob").alias("customer_dob"),
        F.col("email").alias("customer_email"),
        F.col("address_state").alias("customer_state"),
        F.col("occupation").alias("customer_occupation"),
    )
)

# For agents: small (75 rows)
agents_for_join = (
    agents.select(
        F.col("agent_id"),
        F.col("first_name").alias("agent_first_name"),
        F.col("last_name").alias("agent_last_name"),
        F.col("agency_name"),
        F.col("agency_state"),
        F.col("commission_rate").alias("agent_commission_rate"),
    )
)

print("Column subsets prepared.")
print(f"Policies for join: {len(policies_for_join.columns)} cols")
print(f"Customers for join: {len(customers_for_join.columns)} cols")
print(f"Agents for join: {len(agents_for_join.columns)} cols")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join

# COMMAND ----------

# Join 1: claims ←→ policies on id = fema_claim_id
step1 = claims_for_join.join(
    policies_for_join,
    claims_for_join["id"] == policies_for_join["fema_claim_id"],
    how="inner",
).drop("fema_claim_id")  # redundant after join

# Join 2: + customers on customer_id
step2 = step1.join(
    customers_for_join,
    on="customer_id",
    how="inner",
)

# Join 3: + agents on agent_id (broadcast the small one)
enriched = step2.join(
    F.broadcast(agents_for_join),
    on="agent_id",
    how="inner",
)

# Add silver audit columns for this enrichment pass
enriched_final = (
    enriched
    .withColumn("_enrichment_ingested_at", F.lit(INGESTED_AT).cast("timestamp"))
    .withColumn("_enrichment_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
)

print("Joins assembled (lazy). Triggering count to materialize...")
final_count = enriched_final.count()
print(f"Enriched row count: {final_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assert row count

# COMMAND ----------

EXPECTED = 2_721_780

assert final_count == EXPECTED, (
    f"Row count mismatch! Expected {EXPECTED:,} got {final_count:,}"
    f"This indicates orphan FKs between silver tables - investigate."
)
print(f"Row count verified: {final_count:,} == expected {EXPECTED:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to silver

# COMMAND ----------

(
    enriched_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.claims_enriched")
)

print(f"Wrote silver.claims_enriched ({final_count:,} rows, {len(enriched_final.columns)} cols)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification Queries

# COMMAND ----------

spark.sql(
    """
    SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS dinstinct_ids
    FROM silver.claims_enriched
    """
).display()

# spot check
spark.sql("""
    SELECT
        id,
        dateOfLoss,
        state,
        amountPaidOnBuildingClaim,
        policy_number,
        policy_effective_date,
        policy_annual_premium,
        customer_first_name,
        customer_last_name,
        agency_name,
        agent_commission_rate
    FROM silver.claims_enriched
    WHERE dateOfLoss IS NOT NULL
    ORDER BY dateOfLoss DESC
    LIMIT 5
""").display()
  
# Agggregate verification
spark.sql("""
    SELECT
        floodEvent,
        state,
        COUNT(*) AS claim_count,
        ROUND(SUM(amountPaidOnBuildingClaim) / 1e6,2) AS total_paid_millions
    FROM silver.claims_enriched
    WHERE floodEvent IN ('Hurricane Katrina', 'Hurricane Sandy', 'Hurricane Harvey', 'Hurricane Ian')
    GROUP BY floodEvent, state
    ORDER BY total_paid_millions DESC NULLS LAST
    LIMIT 20        
""").display()