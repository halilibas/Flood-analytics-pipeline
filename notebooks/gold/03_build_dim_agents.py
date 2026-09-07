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
    .withColumn("agent_key", F.xxhash64(F.col("agent_id")))

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

