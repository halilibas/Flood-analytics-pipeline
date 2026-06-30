# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - dim agent (SCD Type 1)

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.agents_clean"
TARGET_TABLE = "gold.dim_agent"

PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)

print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Load and Inspect Source

# COMMAND ----------

df_silver = spark.table(SOURCE_TABLE)
print(f"silver.agents_clean: {df_silver.count():,} rows")
df_silver.printSchema()
df_silver.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Build dim_agents

# COMMAND ----------

dim_agent = (
    df_silver
    .withColumn("agent_key", F.monotonically_increasing_id())

    # Project + rename for the dim contract
    .select(
        "agent_key",
        "agent_id",                                    
        F.col("first_name").alias("agent_first_name"),
        F.col("last_name").alias("agent_last_name"),
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("agent_full_name"),
        "agency_name",
        "agency_state",
        "email",
        "phone",
        "hire_date",
        "commission_rate",
    )

    # Audit columns
    .withColumn("_dim_built_at", F.lit(INGESTED_AT).cast("timestamp"))
    .withColumn("_dim_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
)

print(f"dim_agent columns: {len(dim_agent.columns)}")
dim_agent.printSchema()
dim_agent.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanity Check Before Writing

# COMMAND ----------

# verify uniqueness of natural key and surrogate key
n_total = dim_agent.count()
n_distinct_natural = dim_agent.select("agent_id").distinct().count()
n_distinct_surrogate = dim_agent.select("agent_key").distinct().count()

print(f"Total rows:              {n_total}")
print(f"Distinct natural keys:   {n_distinct_natural}")
print(f"Distinct surrogate keys: {n_distinct_surrogate}")

assert n_total == n_distinct_natural, "Duplicate natural key (agent_id) detected"
assert n_total == n_distinct_surrogate, "Duplicate surrogate key (agent_key) detected"
print("\nAll keys verified unique")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write To Gold

# COMMAND ----------

(
    dim_agent.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Wrote {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification Queries

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) AS n FROM {TARGET_TABLE}").display()

spark.sql(f"DESCRIBE {TARGET_TABLE}").show(20, truncate=False)
# agency state distribution should math generator design
spark.sql(f"""
    SELECT agency_state, COUNT(*) AS agent_count
    FROM {TARGET_TABLE}
    GROUP BY agency_state
    ORDER BY agent_count DESC
""").display()

# sample 3 agents to eyeball
spark.sql(f"""
    SELECT agent_key, agent_id, agent_full_name, agency_name, agency_state, commission_rate
    FROM {TARGET_TABLE}
    LIMIT 5
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstrate SCD1 Update Semantics

# COMMAND ----------

# Demonstrate SCD1 update behavior: pick an agent, simulate a commission rate change


# pick one agent_id to use as the simulation target
target_agent = spark.sql(f"""
    SELECT agent_id, agent_full_name, commission_rate
    FROM {TARGET_TABLE}
    LIMIT 1
""").collect()[0]

print("Before simulated change:")
print(f"  agent_id:        {target_agent.agent_id}")
print(f"  name:            {target_agent.agent_full_name}")
print(f"  commission_rate: {target_agent.commission_rate}")


NEW_COMMISSION_RATE = 0.2000  # bump to 20%

df_silver_patched = df_silver.withColumn(
    "commission_rate",
    F.when(F.col("agent_id") == target_agent.agent_id, F.lit(NEW_COMMISSION_RATE))
     .otherwise(F.col("commission_rate"))
)

# rebuild dim_agent from patched silver 
dim_agent_v2 = (
    df_silver_patched
    .withColumn("agent_key", F.monotonically_increasing_id())
    .select(
        "agent_key", "agent_id",
        F.col("first_name").alias("agent_first_name"),
        F.col("last_name").alias("agent_last_name"),
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("agent_full_name"),
        "agency_name", "agency_state", "email", "phone", "hire_date", "commission_rate",
    )
    .withColumn("_dim_built_at", F.lit(INGESTED_AT).cast("timestamp"))
    .withColumn("_dim_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
)

# overwrite — this is the SCD1 semantic
(
    dim_agent_v2.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# verify
after = spark.sql(f"""
    SELECT agent_id, agent_full_name, commission_rate
    FROM {TARGET_TABLE}
    WHERE agent_id = '{target_agent.agent_id}'
""").collect()[0]

print("\nAfter SCD1 overwrite:")
print(f"  commission_rate: {after.commission_rate}  ← old value ({target_agent.commission_rate}) is GONE, no history")

n_after = spark.table(TARGET_TABLE).count()
print(f"\nTotal rows: {n_after} (still 75 and SCD1 doesn't preserve history)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restore the dim to the real state

# COMMAND ----------

# Restore to clean state 
dim_agent_clean = (
    df_silver
    .withColumn("agent_key", F.monotonically_increasing_id())
    .select(
        "agent_key", "agent_id",
        F.col("first_name").alias("agent_first_name"),
        F.col("last_name").alias("agent_last_name"),
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("agent_full_name"),
        "agency_name", "agency_state", "email", "phone", "hire_date", "commission_rate",
    )
    .withColumn("_dim_built_at", F.lit(INGESTED_AT).cast("timestamp"))
    .withColumn("_dim_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
)

(
    dim_agent_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f" {TARGET_TABLE} restored to clean state (75 rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect Delta History

# COMMAND ----------

# Show Delta transaction log
spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}").display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Travel Queries

# COMMAND ----------

print("Current state of target agent:")
target_id = target_agent.agent_id
spark.sql(f"""
    SELECT agent_id, agent_full_name, commission_rate
    FROM {TARGET_TABLE}
    WHERE agent_id = '{target_id}'
""").display()

print("\nVersion 1 state (during simulated change):")
spark.sql(f"""
    SELECT agent_id, agent_full_name, commission_rate
    FROM {TARGET_TABLE} VERSION AS OF 1
    WHERE agent_id = '{target_id}'
""").display()