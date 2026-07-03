# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - dim_customer (SCD Type 2)

# COMMAND ----------

from datetime import datetime, timezone, date
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, BooleanType

SOURCE_TABLE = "silver.customers_clean"
TARGET_TABLE = "gold.dim_customer"

PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)
EFFECTIVE_NOW = date.today()

TRACKED_COLS = ["address_state"]

print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")
print(f"Tracked: {TRACKED_COLS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build initial dim_customer

# COMMAND ----------

df_silver = spark.table(SOURCE_TABLE)
print(f"silver.customers_clean: {df_silver.count():,} rows")

dim_customer_initial = (
    df_silver
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "dob",
        "email",
        "phone",
        "address_line_1",
        "address_state",
        "occupation",
    )
    .withColumn("customer_version", F.lit(1))
    
    # xxhash64 surrogate again
    .withColumn(
        "customer_key",
        F.xxhash64(F.col("customer_id"), F.col("customer_version").cast("string"))
    )

    # derived: full name for display
    .withColumn(
        "customer_full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
    )

    # SCD2 mechanics
    .withColumn("effective_date", F.lit(EFFECTIVE_NOW).cast(DateType()))
    .withColumn("expiration_date", F.lit(None).cast(DateType()))
    .withColumn("is_current", F.lit(True).cast(BooleanType()))

    # audit
    .withColumn("_dim_built_at", F.lit(INGESTED_AT).cast("timestamp"))
    .withColumn("_dim_pipeline_run_id", F.lit(PIPELINE_RUN_ID))

    # final ordering
    .select(
        "customer_key", "customer_id", "customer_version",
        "first_name", "last_name", "customer_full_name",
        "dob", "email", "phone",
        "address_line_1", "address_state",
        "occupation",
        "effective_date", "expiration_date", "is_current",
        "_dim_built_at", "_dim_pipeline_run_id",
    )
)

print(f"dim_customer_initial: {dim_customer_initial.count():,} rows × {len(dim_customer_initial.columns)} cols")
dim_customer_initial.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Uniqueness

# COMMAND ----------

n_total = dim_customer_initial.count()
n_distinct_natural = dim_customer_initial.select("customer_id").distinct().count()
n_distinct_surrogate = dim_customer_initial.select("customer_key").distinct().count()

print(f"Total rows:     {n_total:,}")
print(f"Distinct natural keys: {n_distinct_natural:,}")
print(f"Distinct surrogate keys: {n_distinct_surrogate:,}")

assert n_total == n_distinct_natural, "Duplicate customer_id in intial load"
assert n_total == n_distinct_surrogate, "Duplicate customer_key in intial load"
print("Initial load uniqueness verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write initial dim

# COMMAND ----------

(
    dim_customer_initial.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print("Wrote {TARGET_TABLE} {initial_load}")

spark.sql(f"SELECT COUNT(*) AS n, SUM(CAST(is_current AS INT)) AS n_current FROM {TARGET_TABLE}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE STEP 1

# COMMAND ----------

def changed_expr(silver_alias="s", dim_alias="d"):
    return " OR ".join(
        f"{silver_alias}.{c} IS DISTINCT FROM {dim_alias}.{c}" for c in TRACKED_COLS
    )

changed_condition = changed_expr("s", "d")

merge_step_1_sql = f"""
MERGE INTO {TARGET_TABLE} AS d
USING (
    SELECT customer_id, address_state
    FROM {SOURCE_TABLE}    
) AS s
ON d.customer_id = s.customer_id
    AND d.is_current = TRUE
    AND ({changed_condition})
WHEN MATCHED THEN UPDATE SET
    d.expiration_date = DATE('{EFFECTIVE_NOW}'),
    d.is_current = FALSE
"""

result_1 = spark.sql(merge_step_1_sql)
result_1.display()
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE Step 2

# COMMAND ----------

merge_step_2_sql = f"""
MERGE INTO {TARGET_TABLE} AS d
USING (
    WITH max_version AS (
        SELECT customer_id, COALESCE(MAX(customer_version), 0) AS prev_version
        FROM {TARGET_TABLE}
        GROUP BY customer_id
    )
    SELECT
        s.customer_id,
        s.first_name, s.last_name,
        s.dob, s.email, s.phone,
        s.address_line_1, s.address_state,
        s.occupation,
        COALESCE(mv.prev_version, 0) + 1 AS new_version
    FROM {SOURCE_TABLE} s
    LEFT JOIN max_version mv ON s.customer_id = mv.customer_id
    WHERE NOT EXISTS (
        SELECT 1 FROM {TARGET_TABLE} d2
        WHERE d2.customer_id = s.customer_id
          AND d2.is_current = TRUE
    )
) AS src
ON FALSE
WHEN NOT MATCHED THEN INSERT (
    customer_key, customer_id, customer_version,
    first_name, last_name, customer_full_name,
    dob, email, phone,
    address_line_1, address_state,
    occupation,
    effective_date, expiration_date, is_current,
    _dim_built_at, _dim_pipeline_run_id
) VALUES (
    XXHASH64(src.customer_id, CAST(src.new_version AS STRING)),
    src.customer_id, src.new_version,
    src.first_name, src.last_name, CONCAT_WS(' ', src.first_name, src.last_name),
    src.dob, src.email, src.phone,
    src.address_line_1, src.address_state,
    src.occupation,
    DATE('{EFFECTIVE_NOW}'), NULL, TRUE,
    TIMESTAMP('{INGESTED_AT.isoformat()}'), '{PIPELINE_RUN_ID}'
)
"""

result_2 = spark.sql(merge_step_2_sql)
result_2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify no-op MERGE

# COMMAND ----------

post_merge_count = spark.table(TARGET_TABLE).count()
post_merge_current = spark.sql(f"SELECT COUNT(*) AS n FROM {TARGET_TABLE} WHERE is_current = TRUE").collect()[0]["n"]

print(f"Total rows: {post_merge_count: ,} expected 2,721,780")
print(f"Current rows: {post_merge_current:,} expected 2,721,780")
print(f"Closed rows: {post_merge_count - post_merge_current}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulated address change (demonstrate SCD2 woks)

# COMMAND ----------

#pick a customer, simulate address_state change from their current to CA
target = spark.sql(f"""
    SELECT customer_id, address_state, customer_full_name
    FROM {TARGET_TABLE}
    WHERE is_current = TRUE
    LIMIT 1
""").collect()[0]

target_customer_id = target.customer_id
old_state = target.address_state
new_state = "WY" if old_state == "CA" else "CA"

print(f"Target customer: {target_customer_id}")
print(f"  Name: {target.customer_full_name}")
print(f"  Address state: {old_state} → {new_state}")

# Build incoming
incoming = spark.sql(f"""
    SELECT
        customer_id, first_name, last_name,
        dob, email, phone,
        address_line_1,
        '{new_state}' AS address_state,
        occupation
    FROM {SOURCE_TABLE}
    WHERE customer_id = '{target_customer_id}'
""")

incoming.createOrReplaceTempView("incoming_customer_change")
incoming.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run MERGE with simulated change

# COMMAND ----------

# Step 1 with simulated source
merge_1_sim = f"""
MERGE INTO {TARGET_TABLE} AS d
USING incoming_customer_change AS s
ON  d.customer_id = s.customer_id
    AND d.is_current = TRUE
    AND (1=1)
WHEN MATCHED THEN UPDATE SET
    d.expiration_date = DATE('{EFFECTIVE_NOW}'),
    d.is_current = FALSE
"""

print("=== Simulated change — Step 1 ===")
spark.sql(merge_1_sim).display()

# Step 2 with simulated source
merge_2_sim = f"""
MERGE INTO {TARGET_TABLE} AS d
USING (
    WITH max_version AS (
        SELECT customer_id, COALESCE(MAX(customer_version), 0) AS prev_version
        FROM {TARGET_TABLE}
        GROUP BY customer_id
    )
    SELECT
        s.customer_id,
        s.first_name, s.last_name,
        s.dob, s.email, s.phone,
        s.address_line_1, s.address_state,
        s.occupation,
        COALESCE(mv.prev_version, 0) + 1 AS new_version
    FROM incoming_customer_change s
    LEFT JOIN max_version mv ON s.customer_id = mv.customer_id
    WHERE NOT EXISTS (
        SELECT 1 FROM {TARGET_TABLE} d2
        WHERE d2.customer_id = s.customer_id
          AND d2.is_current = TRUE
    )
) AS src
ON FALSE
WHEN NOT MATCHED THEN INSERT (
    customer_key, customer_id, customer_version,
    first_name, last_name, customer_full_name,
    dob, email, phone,
    address_line_1, address_state,
    occupation,
    effective_date, expiration_date, is_current,
    _dim_built_at, _dim_pipeline_run_id
) VALUES (
    XXHASH64(src.customer_id, CAST(src.new_version AS STRING)),
    src.customer_id, src.new_version,
    src.first_name, src.last_name, CONCAT_WS(' ', src.first_name, src.last_name),
    src.dob, src.email, src.phone,
    src.address_line_1, src.address_state,
    src.occupation,
    DATE('{EFFECTIVE_NOW}'), NULL, TRUE,
    TIMESTAMP('{INGESTED_AT.isoformat()}'), '{PIPELINE_RUN_ID}'
)
"""

print("Simulated change — Step 2 ")
spark.sql(merge_2_sim).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the SCD2 outcome

# COMMAND ----------

# show both versions for target customer
spark.sql(f"""
    SELECT customer_key, customer_id, customer_version,
           address_state, effective_date, expiration_date, is_current
    FROM {TARGET_TABLE}
    WHERE customer_id = '{target_customer_id}'
    ORDER BY customer_version
""").display()

# global invariants
new_total = spark.table(TARGET_TABLE).count()
print(f"Total rows: {new_total:,}  (expected 2,721,781)")

multiple_current = spark.sql(f"""
    SELECT customer_id, COUNT(*) AS n
    FROM {TARGET_TABLE}
    WHERE is_current = TRUE
    GROUP BY customer_id
    HAVING COUNT(*) > 1
""").count()
print(f"Customers with >1 current row: {multiple_current}  (must be 0)")

#global surrogate key uniqueness
n_distinct = spark.sql(f"SELECT COUNT(DISTINCT customer_key) AS n FROM {TARGET_TABLE}").collect()[0]["n"]
print(f"Distinct customer_key: {n_distinct:,}  (should equal {new_total:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 