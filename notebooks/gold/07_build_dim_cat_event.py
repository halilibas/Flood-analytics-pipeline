# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - dim_cat_event (SCD Type 1)

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.claims_clean"
TARGET_TABLE = "gold.dim_cat_event"

PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Derive dim_cat_event

# COMMAND ----------

silver = spark.table(SOURCE_TABLE)

distinct_events = (
    silver
    .select(F.col("floodEvent").alias("event_name"))
    .distinct()

    .filter(F.col("event_name").isNotNull())
)

print(f"Distinct events: {distinct_events.count()}")

# categorize
dim_cat_event = (
    distinct_events
    .withColumn(
        "event_type",
        F.when(F.col("event_name").startswith("Hurricane"), F.lit("Hurricane"))
         .when(F.col("event_name").startswith("Tropical Storm"), F.lit("Tropical Storm"))
         .when(F.col("event_name").isin(["Flooding", "Not a named storm"]), F.lit("UNNAMED"))
         .otherwise(F.lit("Other"))
    )
    .withColumn(
        "is_named_storm",
        F.col("event_type").isin(["Hurricane", "Tropical Storm"])
    )
    .withColumn(
        "cat_event_key",
        F.xxhash64(F.col("event_name"))
    )
    .withColumn("_dim_built_at", F.lit(INGESTED_AT).cast("timestamp"))
    .withColumn("_dim_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
    .select(
        "cat_event_key",
        "event_name",
        "event_type",
        "is_named_storm",
        "_dim_built_at",
        "_dim_pipeline_run_id",
    )

)

# verify
n_rows = dim_cat_event.count()
n_distinct_names = dim_cat_event.select("event_name").distinct().count()
n_distinct_keys = dim_cat_event.select("cat_event_key").distinct().count()

print(f"Rows:   {n_rows}")
print(f"Distinct event names: {n_distinct_names}")
print(f"Distinct cat_event_key:     {n_distinct_keys}")

assert n_rows == n_distinct_keys, "Hash collision in cat_event_key"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write and Verify

# COMMAND ----------

(
    dim_cat_event.write
    .format("delta")
    .mode("overwrite")
    .option("overWriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Wrote {TARGET_TABLE}")

# type distribution
spark.sql(f"""
    SELECT event_type, COUNT(*) AS n
    FROM {TARGET_TABLE}
    GROUP BY event_type
    ORDER BY n DESC
""").display()

# sample named storms
spark.sql(f"""
    SELECT event_name, event_type, is_named_storm
    FROM {TARGET_TABLE}
    WHERE event_type = 'Hurricane'
    ORDER BY event_name
    LIMIT 20
""").display()

# sample unnamed
spark.sql(f"""
    SELECT * FROM {TARGET_TABLE}
    WHERE event_type = 'UNNAMED'
""").display()


