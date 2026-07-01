# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - dim policy (SCD Type 2)

# COMMAND ----------

from datetime import datetime, timezone, date
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, BooleanType, LongType

SOURCE_TABLE = "silver.policies_clean"
TARGET_TABLE = "gold.dim_policy"

PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)


EFFECTIVE_NOW = date.today()

print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")
print(f"Effective-now date: {EFFECTIVE_NOW}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build the initial dim_policy

# COMMAND ----------

df_silver = spark.table(SOURCE_TABLE)
print(f"silver.policies_clean: {df_silver.count():,} rows")


dim_policy_initial = (
    df_silver
    .select(
        F.col("policy_number"),
        F.col("customer_id"),
        F.col("agent_id"),
        F.col("building_coverage"),
        F.col("contents_coverage"),
        F.col("deductible_amount"),
        F.col("annual_premium"),
        F.col("coverage_type"),
        F.col("effective_date"),                          
    )
    # surrogate key
    .withColumn("policy_key", F.monotonically_increasing_id())
    # SCD2 mechanics
    .withColumn("expiration_date", F.lit(None).cast(DateType()))
    .withColumn("is_current", F.lit(True).cast(BooleanType()))
    .withColumn("policy_version", F.lit(1))
    # audit
    .withColumn("_dim_built_at", F.lit(INGESTED_AT).cast("timestamp"))
    .withColumn("_dim_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
    # final column ordering
    .select(
        "policy_key", "policy_number", "policy_version",
        "customer_id", "agent_id",
        "building_coverage", "contents_coverage", "deductible_amount",
        "annual_premium", "coverage_type",
        "effective_date", "expiration_date", "is_current",
        "_dim_built_at", "_dim_pipeline_run_id",
    )
)

print(f"dim_policy_initial: {dim_policy_initial.count():,} rows × {len(dim_policy_initial.columns)} cols")
dim_policy_initial.printSchema()
dim_policy_initial.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Uniquieness Assumptions

# COMMAND ----------

n_total = dim_policy_initial.count()
n_distinct_natural = dim_policy_initial.select("policy_number").distinct().count()
n_distinct_surrogate = dim_policy_initial.select("policy_key").distinct().count()

print(f"Total rows:              {n_total:,}")
print(f"Distinct natural keys:   {n_distinct_natural:,}")
print(f"Distinct surrogate keys: {n_distinct_surrogate:,}")


assert n_total == n_distinct_natural, "Duplicate policy_number in initial load"
assert n_total == n_distinct_surrogate, "Duplicate policy_key in initial load"
print("\n✓ Initial load uniqueness verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Initial dim_policy

# COMMAND ----------

(
    dim_policy_initial.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Wrote {TARGET_TABLE} (initial load)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick Sanity Queries

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) AS n, SUM(CAST(is_current AS INT)) AS n_current FROM {TARGET_TABLE}").display()


spark.sql(f"DESCRIBE {TARGET_TABLE}").show(20, truncate=False)

spark.sql(f"""
    SELECT policy_key, policy_number, policy_version,
           annual_premium, building_coverage, is_current,
           effective_date, expiration_date
    FROM {TARGET_TABLE}
    LIMIT 5
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD2 MERGE Strategy
# MAGIC
# MAGIC ### Delta MERGE handles two of the three SCD2 cases natively. The third(closing the old version + opening a new one on the same MERGE call) is the tricky one. Pattern used: **two-step MERGE**.
# MAGIC
# MAGIC ### For every silver policy whose tracked attributes differ from the current dim version, MERGE updates the existing dim row: set `expiration_date = today` and `is_current = false`.
# MAGIC
# MAGIC ### Step 2: Insert new versions for every silver policy that either (a) doesn't exist in dim yet, or (b) just had its version closed in Step 1, INSERT a fresh row with` is_current = true`, `effective_date = today`, `expiration_date = NULL` policy_version = previous + 1` (or 1 for brand new).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper; define what "changed" means

# COMMAND ----------

# Tracked attributes — change → new version
TRACKED_COLS = [
    "annual_premium",
    "building_coverage",
    "contents_coverage",
    "deductible_amount",
    "coverage_type",
]

def changed_expr(silver_alias="s", dim_alias="d"):
    conditions = []
    for col in TRACKED_COLS:
        conditions.append(f"{silver_alias}.{col} IS DISTINCT FROM {dim_alias}.{col}")
    return " OR ".join(conditions)

print("Tracked columns:", TRACKED_COLS)
print("\nChanged-detection SQL:")
print(changed_expr())

# COMMAND ----------

# MAGIC %md
# MAGIC ### The MERGE itself

# COMMAND ----------

# Step 1 of the two-step MERGE: close versions whose tracked attrs changed
# We match on policy_number, only consider current dim rows (is_current=true),
# and update the matched row to close it.

changed_condition = changed_expr("s", "d")

merge_step_1_sql = f"""
MERGE INTO {TARGET_TABLE} AS d
USING (
    SELECT
        policy_number,
        customer_id,
        agent_id,
        building_coverage,
        contents_coverage,
        deductible_amount,
        annual_premium,
        coverage_type,
        effective_date
    FROM {SOURCE_TABLE}
) AS s
ON  d.policy_number = s.policy_number
    AND d.is_current = TRUE
    AND ({changed_condition})
WHEN MATCHED THEN UPDATE SET
    d.expiration_date = DATE('{EFFECTIVE_NOW}'),
    d.is_current = FALSE
"""

print("=== Step 1 MERGE SQL ===")
print(merge_step_1_sql)


result_1 = spark.sql(merge_step_1_sql)
result_1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE Step 2: insert new versions

# COMMAND ----------

# Step 2: for every silver policy that either doesn't have a current dim row
# (because we just closed it, OR because it never existed), insert a v_next.

# We need to figure out the next version number per policy.
# Sub-query: for each policy_number, find max(policy_version) currently in dim.

merge_step_2_sql = f"""
MERGE INTO {TARGET_TABLE} AS d
USING (
    WITH max_version AS (
        SELECT policy_number, COALESCE(MAX(policy_version), 0) AS prev_version
        FROM {TARGET_TABLE}
        GROUP BY policy_number
    )
    SELECT
        s.policy_number,
        s.customer_id,
        s.agent_id,
        s.building_coverage,
        s.contents_coverage,
        s.deductible_amount,
        s.annual_premium,
        s.coverage_type,
        s.effective_date AS silver_effective_date,
        COALESCE(mv.prev_version, 0) + 1 AS new_version
    FROM {SOURCE_TABLE} s
    LEFT JOIN max_version mv ON s.policy_number = mv.policy_number
    WHERE NOT EXISTS (
        SELECT 1 FROM {TARGET_TABLE} d2
        WHERE d2.policy_number = s.policy_number
          AND d2.is_current = TRUE
    )
) AS src
ON FALSE  -- always-false match means every row goes to NOT MATCHED branch
WHEN NOT MATCHED THEN INSERT (
    policy_key,
    policy_number, policy_version,
    customer_id, agent_id,
    building_coverage, contents_coverage, deductible_amount,
    annual_premium, coverage_type,
    effective_date, expiration_date, is_current,
    _dim_built_at, _dim_pipeline_run_id
) VALUES (
    MONOTONICALLY_INCREASING_ID(),
    src.policy_number, src.new_version,
    src.customer_id, src.agent_id,
    src.building_coverage, src.contents_coverage, src.deductible_amount,
    src.annual_premium, src.coverage_type,
    DATE('{EFFECTIVE_NOW}'), NULL, TRUE,
    TIMESTAMP('{INGESTED_AT.isoformat()}'), '{PIPELINE_RUN_ID}'
)
"""

print("=== Step 2 MERGE SQL ===")
print(merge_step_2_sql)

result_2 = spark.sql(merge_step_2_sql)
result_2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the MERGE was a no-op on initial run

# COMMAND ----------


post_merge_count = spark.table(TARGET_TABLE).count()
post_merge_current = spark.sql(f"SELECT COUNT(*) AS n FROM {TARGET_TABLE} WHERE is_current = TRUE").collect()[0]["n"]

print(f"Total rows:        {post_merge_count:,}  (expected ~2,721,780)")
print(f"Current rows:      {post_merge_current:,}  (expected = total)")
print(f"Closed rows:       {post_merge_count - post_merge_current:,}  (expected 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulate a Policy Change In Silver

# COMMAND ----------

# Pick one policy_number; bump its annual_premium and re-merge

target_policy = spark.sql(f"""
    SELECT policy_number, annual_premium, building_coverage
    FROM {TARGET_TABLE}
    WHERE is_current = TRUE
    LIMIT 1
""").collect()[0]

target_policy_number = target_policy.policy_number
old_premium = target_policy.annual_premium

print(f"Target policy: {target_policy_number}")
print(f"  Current premium:  ${old_premium}")
print(f"  Current coverage: ${target_policy.building_coverage}")


NEW_PREMIUM = float(old_premium) * 1.10  


incoming = spark.sql(f"""
    SELECT
        policy_number, customer_id, agent_id,
        building_coverage, contents_coverage, deductible_amount,
        CAST({NEW_PREMIUM:.2f} AS DECIMAL(10, 2)) AS annual_premium,
        coverage_type,
        effective_date
    FROM {SOURCE_TABLE}
    WHERE policy_number = '{target_policy_number}'
""")

incoming.createOrReplaceTempView("incoming_silver_change")
incoming.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the MERGE Steps with the simulated change

# COMMAND ----------

merge_step_1_sim = f"""
MERGE INTO {TARGET_TABLE} AS d
USING incoming_silver_change AS s
ON  d.policy_number = s.policy_number
    AND d.is_current = TRUE
    AND ({changed_condition})
WHEN MATCHED THEN UPDATE SET
    d.expiration_date = DATE('{EFFECTIVE_NOW}'),
    d.is_current = FALSE
"""

print("=== Simulated change — Step 1: close old version ===")
spark.sql(merge_step_1_sim).display()

merge_step_2_sim = f"""
MERGE INTO {TARGET_TABLE} AS d
USING (
    WITH max_version AS (
        SELECT policy_number, COALESCE(MAX(policy_version), 0) AS prev_version
        FROM {TARGET_TABLE}
        GROUP BY policy_number
    )
    SELECT
        s.policy_number,
        s.customer_id, s.agent_id,
        s.building_coverage, s.contents_coverage, s.deductible_amount,
        s.annual_premium, s.coverage_type,
        s.effective_date AS silver_effective_date,
        COALESCE(mv.prev_version, 0) + 1 AS new_version
    FROM incoming_silver_change s
    LEFT JOIN max_version mv ON s.policy_number = mv.policy_number
    WHERE NOT EXISTS (
        SELECT 1 FROM {TARGET_TABLE} d2
        WHERE d2.policy_number = s.policy_number
          AND d2.is_current = TRUE
    )
) AS src
ON FALSE
WHEN NOT MATCHED THEN INSERT (
    policy_key,
    policy_number, policy_version,
    customer_id, agent_id,
    building_coverage, contents_coverage, deductible_amount,
    annual_premium, coverage_type,
    effective_date, expiration_date, is_current,
    _dim_built_at, _dim_pipeline_run_id
) VALUES (
    MONOTONICALLY_INCREASING_ID(),
    src.policy_number, src.new_version,
    src.customer_id, src.agent_id,
    src.building_coverage, src.contents_coverage, src.deductible_amount,
    src.annual_premium, src.coverage_type,
    DATE('{EFFECTIVE_NOW}'), NULL, TRUE,
    TIMESTAMP('{INGESTED_AT.isoformat()}'), '{PIPELINE_RUN_ID}'
)
"""

print("=== Simulated change — Step 2: insert new version ===")
spark.sql(merge_step_2_sim).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the SCD2 outcome

# COMMAND ----------

# Now query the target policy should see two rows: v1 closed, v2 current
spark.sql(f"""
    SELECT
        policy_key, policy_number, policy_version,
        annual_premium, building_coverage,
        effective_date, expiration_date, is_current
    FROM {TARGET_TABLE}
    WHERE policy_number = '{target_policy_number}'
    ORDER BY policy_version
""").display()


new_total = spark.table(TARGET_TABLE).count()
print(f"Total rows after change:  {new_total:,}  (expected {2_721_780 + 1:,})")


multiple_current_check = spark.sql(f"""
    SELECT policy_number, COUNT(*) AS current_count
    FROM {TARGET_TABLE}
    WHERE is_current = TRUE
    GROUP BY policy_number
    HAVING COUNT(*) > 1
""").count()
print(f"Policies with >1 current row: {multiple_current_check}  (must be 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix Surrogate Key Collision

# COMMAND ----------

from pyspark.sql import functions as F

dim_repaired = (
    spark.table(TARGET_TABLE)
    .withColumn(
        "policy_key",
        F.xxhash64(F.col("policy_number"), F.col("policy_version").cast("string"))
    )
)

(
    dim_repaired.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)


spark.sql(f"""
    SELECT policy_key, policy_number, policy_version, is_current
    FROM {TARGET_TABLE}
    WHERE policy_number = '{target_policy_number}'
    ORDER BY policy_version
""").display()

n_rows = spark.table(TARGET_TABLE).count()
n_distinct_keys = spark.sql(f"SELECT COUNT(DISTINCT policy_key) AS n FROM {TARGET_TABLE}").collect()[0]["n"]
print(f"Rows: {n_rows:,}")
print(f"Distinct policy_key: {n_distinct_keys:,}")
assert n_rows == n_distinct_keys, "policy_key still colliding"
print("policy_key now unique per version")