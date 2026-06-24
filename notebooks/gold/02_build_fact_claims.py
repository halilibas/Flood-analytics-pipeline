# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - fact_claims v0

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, LongType, DecimalType

SOURCE_TABLE = "silver.claims_enriched"
DIM_DATE_TABLE = "gold.dim_date"
TARGET_TABLE = "gold.fact_claims"

PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)

print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Source

# COMMAND ----------

claims = spark.table (SOURCE_TABLE)
dim_date = spark.table(DIM_DATE_TABLE)

print(f"Source claims: {claims.count():,}")
print(f"dim_date: {dim_date.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build Date FK via Join

# COMMAND ----------

claims_with_date_fk = claims.join(
    dim_date.select(F.col("date").alias("_d"), F.col("date_key").alias("date_of_loss_key")),
    claims["dateOfLoss"] == F.col("_d"),
    how="left",
).drop("_d")

# verify any unmatched dates
unmatched = claims_with_date_fk.filter(
    F.col("dateOfLoss").isNotNull() & F.col("date_of_loss_key").isNull()
).count()
print(f"Claims with dateOfLoss but no date_key match: {unmatched}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate surrogate key + select columns
# MAGIC

# COMMAND ----------

fact = (
    claims_with_date_fk
    .drop("fema_claim_id")
    # surrogate key
    .withColumn("claim_key", F.monotonically_increasing_id())
    
    # degenerate dimension, rename id to be explicit
    .withColumnRenamed("id", "fema_claim_id")
    
    # claim count for aggregations
    .withColumn("claim_count", F.lit(1).cast(IntegerType()))
    
    # total paid = building + contents
    .withColumn(
        "total_claim_amount",
        F.coalesce(F.col("amountPaidOnBuildingClaim"), F.lit(0))
        + F.coalesce(F.col("amountPaidOnContentsClaim"), F.lit(0))
        + F.coalesce(F.col("amountPaidOnIncreasedCostOfComplianceClaim"), F.lit(0))
    )

    # audit columns
    .withColumn("_fact_built_at", F.lit(INGESTED_AT).cast("timestamp"))
    .withColumn("_fact_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Select Final fact_claims Columns

# COMMAND ----------

fact_final = fact.select(
    "claim_key",  #surrogate 
    "fema_claim_id", 
    "date_of_loss_key",

    # measures - paid
    F.col("amountPaidOnBuildingClaim").alias("building_claim_amount"),
    F.col("amountPaidOnContentsClaim").alias("contents_claim_amount"),
    F.col("amountPaidOnIncreasedCostOfComplianceClaim").alias("icc_claim_amount"),
    "total_claim_amount",

    # measures damage
    F.col("buildingDamageAmount").alias("building_damage_amount"),
    F.col("contentsDamageAmount").alias("contents_damage_amount"),

    # measures - coverage
    F.col("totalBuildingInsuranceCoverage").alias("building_coverage_limit"),
    F.col("totalContentsInsuranceCoverage").alias("contents_coverage_limit"),
    F.col("iccCoverage").alias("icc_coverage_limit"),

    #measures - event characteristic
    F.col("waterDepth").alias("water_depth"),

    #fact
    "claim_count",

    # FEMA pre-tagged event name
    F.col("floodEvent").alias("flood_event_name"),

    "state",

    # audit
    "_fact_built_at",
    "_fact_pipeline_run_id",
)

print(f"fact_claims columns: {len(fact_final.columns)}")
fact_final.printSchema()
fact_final.show(5)

# COMMAND ----------

#derive year and partition by that
fact_partitioned = fact_final.withColumn(
    "loss_year",
    (F.col("date_of_loss_key") / 10000).cast(IntegerType())
)

(
    fact_partitioned.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("loss_year")
    .saveAsTable(TARGET_TABLE)
)

print(f"Wrote {TARGET_TABLE} partitioned by loss_year")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification

# COMMAND ----------

# row count
spark.sql(f"SELECT COUNT(*) AS n fROM {TARGET_TABLE}").display()

# schema check
spark.sql(f"DESCRIBE {TARGET_TABLE}").show(40, truncate=False)

# total paid by year
spark.sql(f"""
    SELECT
        d.year,
        COUNT(*) AS claims,
        ROUND(SUM(f.total_claim_amount) / 1e9, 2) AS total_paid_billions
    FROM {TARGET_TABLE} f
    JOIN {DIM_DATE_TABLE} d
        ON f.date_of_loss_key = d.date_key
    WHERE d.year BETWEEN 2000 AND 2025
    GROUP BY d.year
    ORDER BY d.year       
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Two more analytical queries that showcase the model

# COMMAND ----------

# Top 10 CAT events by total paid
spark.sql(f"""
    SELECT
        flood_event_name,
        COUNT(*) AS claim_count,
        ROUND(SUM(total_claim_amount) / 1e9, 2) AS total_paid_billions
    FROM {TARGET_TABLE}
    WHERE flood_event_name IS NOT NULL
    GROUP BY flood_event_name
    ORDER BY total_paid_billions DESC
    LIMIT 10                   
""").display()


# Severity by hurricane season vs non-season
spark.sql(f"""
    SELECT
        d.hurricane_season_flag,
        COUNT(*) AS claims,
        ROUND(AVG(f.total_claim_amount), 0) AS avg_severity,
        ROUND(SUM(f.total_claim_amount) / 1e9, 2) AS total_paid_billions
    FROM {TARGET_TABLE} f
    JOIN {DIM_DATE_TABLE} d ON f.date_of_loss_key = d.date_key
    GROUP BY d.hurricane_season_flag
    ORDER BY total_paid_billions DESC
""").display()