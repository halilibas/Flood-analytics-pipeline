# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Clean FEMA Claims

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4
from pyspark.sql import functions as f
from pyspark.sql.types import DateType, DecimalType, BooleanType, StringType, IntegerType

SOURCE_TABLE = "bronze.fema_claims_raw"
TARGET_TABLE = "silver.claims_clean"

PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)

print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")
print(f"Pipeline run id: {PIPELINE_RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Bronze

# COMMAND ----------

df_bronze = spark.table(SOURCE_TABLE)
print(f"Bronze row count: {df_bronze.count()}")
print(f"Bronze column count: {len(df_bronze.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trim all string columns; convert empty strings to NULL

# COMMAND ----------

string_cols = [f.name for f in df_bronze.schema.fields if isinstance(f.dataType, StringType)]

df_trimmed = df_bronze
for c in string_cols:
    df_trimmed = df_trimmed.withColumn(
        c,
        f.when(f.trim(f.col(c)) == "", None).otherwise(f.trim(f.col(c)))
    )
print(f"Trim + empty to null applied to {len(string_cols)} string columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FEMA date format: "1992-12-11T00:00:00.000Z"
# MAGIC

# COMMAND ----------

DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"

date_cols = ["dateOfLoss", "originalNBDate", "originalConstructionDate", "asOfDate"]

df_dates = df_trimmed
for c in date_cols:
    df_dates = df_dates.withColumn(
        c,
        f.to_date(f.to_timestamp(f.col(c), DATE_FORMAT))
    )
#verify
print("Date parsing sanity check: ")
for c in date_cols:
    n = df_dates.filter(f.col(c).isNotNull()).count()
    print(f"  {c}: {n:,} non-null after parsing")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sentinal Handling

# COMMAND ----------

df_sentinel = df_dates.withColumn(
    "originalConstructionDate",
    f.when(f.col("originalConstructionDate") < f.lit("1900-01-01"), None).otherwise(f.col("originalConstructionDate"))
)

#verify
sentinel_remaining = df_sentinel.filter(
    f.col("originalConstructionDate") < f.lit("1900-01-01")
).count()
print(f"Pre 1900 construction dates after sentinel cleanup: {sentinel_remaining} (should be zero)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### City Redaction Handling

# COMMAND ----------

df_no_city = df_sentinel.drop("reportedCity")
print(f"Dropped reportedCity. Column count: {len(df_no_city.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Numeric Casting

# COMMAND ----------

DECIMAL_TYPE = DecimalType(18, 2)

amount_cols = [
    "amountPaidOnBuildingClaim",
    "amountPaidOnContentsClaim",
    "amountPaidOnIncreasedCostOfComplianceClaim",
    "netBuildingPaymentAmount",
    "netContentsPaymentAmount",
    "netIccPaymentAmount",
    "buildingDamageAmount",
    "contentsDamageAmount",
    "buildingPropertyValue",
    "contentsPropertyValue",
    "totalBuildingInsuranceCoverage",
    "totalContentsInsuranceCoverage",
    "iccCoverage",
    "buildingReplacementCost",
    "contentsReplacementCost",
]

# Columns where negatives indicate bad data and should be nulled
payment_cols_to_clean = [
    "amountPaidOnBuildingClaim",
    "amountPaidOnContentsClaim",
    "amountPaidOnIncreasedCostOfComplianceClaim",
    "netBuildingPaymentAmount",
    "netContentsPaymentAmount",
    "netIccPaymentAmount",
]

df_decimal = df_no_city
for c in amount_cols:
    df_decimal = df_decimal.withColumn(
        c,
        f.col(c).cast(DECIMAL_TYPE))

for c in payment_cols_to_clean:
    df_decimal = df_decimal.withColumn(
        c,
        f.when(f.col(c) < 0, None).otherwise(f.col(c))
    )

#verify

for c in payment_cols_to_clean:
    n_neg = df_decimal.filter(f.col(c) < 0).count()
    print(f"  {c}: {n_neg} negative values remaining")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Other Numeric Casts

# COMMAND ----------

# Integer typed columns
int_cols = [
    "yearOfLoss",
    "numberOfFloorsInTheInsuredBuilding",
    "numberOfUnits",
    "floodWaterDuration",
    "policyCount",
]

# Float/decimal columns that aren't amounts
float_cols = [
    "elevationDifference",
    "baseFloodElevation",
    "lowestAdjacentGrade",
    "lowestFloorElevation",
    "waterDepth",
    "latitude",
    "longitude",
]

df_typed = df_decimal
for c in int_cols:
    df_typed = df_typed.withColumn(
        c,
        f.col(c).cast(IntegerType())
    )

for c in float_cols:
    df_typed = df_typed.withColumn(c, f.col(c).cast("double"))

print("Integer and float casts applied")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Boolean Casting

# COMMAND ----------

# Cast: '1' -> true, '0' -> false, anything else -> NULL

indicator_cols = [
    "agricultureStructureIndicator",
    "elevatedBuildingIndicator",
    "elevationCertificateIndicator",
    "nonProfitIndicator",
    "postFIRMConstructionIndicator",
    "smallBusinessIndicatorBuilding",
    "primaryResidenceIndicator",
    "floodproofedIndicator",
    "stateOwnedIndicator",
    "rentalPropertyIndicator",
    "houseWorship",
    "disasterAssistanceCoverageRequired"
]

df_bool = df_typed
for c in indicator_cols:
    df_bool = df_bool.withColumn(
        c,
        f.when(f.col(c) == "1", f.lit(True))
         .when(f.col(c) == "0", f.lit(False))
         .otherwise(None)
         .cast(BooleanType())
    )

#verify
print("Boolean cast sanity check: ")
for c in indicator_cols[:3]:
    counts = df_bool.groupBy(c).count().collect()
    print(f"  {c}: {[(r[0], r[1]) for r in counts]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### State Normalization
# MAGIC

# COMMAND ----------

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC","PR","VI","GU","AS","MP",  # including territories
}

df_state = df_bool.withColumn(
    "state",
    f.when(f.upper(f.col("state")).isin(US_STATES), f.upper(f.col("state")))
     .otherwise(None)
)

#verify
n_state_null = df_state.filter(f.col("state").isNull()).count()
n_state_total = df_state.count()
print(f"State after normalization: {n_state_null:,} null / {n_state_total:,} ({n_state_null / n_state_total: .2%})")





# COMMAND ----------

# MAGIC %md
# MAGIC ### Dedup on id

# COMMAND ----------

count_before = df_state.count()
df_dedup = df_state.dropDuplicates(["id"])
count_after = df_dedup.count()

print(f"Before dedup: {count_before:,}")
print(f"After dedup: {count_after:,}")
print(f"Duplicated removed: {count_before - count_after:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Silver Audit Columns

# COMMAND ----------

df_final = (
    df_dedup
    .withColumn("_silver_ingested_at", f.lit(INGESTED_AT).cast("timestamp"))
    .withColumn("_silver_pipeline_run_id", f.lit(PIPELINE_RUN_ID))
)

print(f"Final column count: {len(df_final.columns)}")
df_final.select("id", "dateOfLoss", "state", "amountPaidOnBuildingClaim", "elevatedBuildingIndicator", "_silver_ingested_at").limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write To Silver

# COMMAND ----------

(
    df_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Wrote {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification

# COMMAND ----------

#Sanity queries 

print("Row Count: ")
spark.sql(f"SELECT COUNT(*) AS n FROM {TARGET_TABLE}").display()

print("Schema sample: ")
spark.sql(f"DESCRIBE {TARGET_TABLE}").show(40, truncate=False)

print("Date range: ")
spark.sql(f"""
    SELECT
        MIN(dateOfLoss) AS earliest,
        MAX(dateOfLoss) AS latest,
        COUNT(DISTINCT id) AS distinct_claims          
    FROM {TARGET_TABLE}
""").display()

print("Payment column null vs zero distribution")
spark.sql(f"""
    SELECT
        SUM(CASE WHEN amountPaidOnBuildingClaim IS NULL THEN 1 ELSE 0 END) AS n_null,
        SUM(CASE WHEN amountPaidOnBuildingClaim = 0 THEN 1 ELSE 0 END) AS n_zero,
        SUM(CASE WHEN amountPaidOnBuildingClaim > 0 THEN 1 ELSE 0 END) AS n_positive
    FROM {TARGET_TABLE}          
""").display()

print("Top states: ")
spark.sql(f"""
    SELECT
        SUM(CASE WHEN originalConstructionDate IS NULL THEN 1 ELSE 0 END) AS n_nulll,
        SUM(CASE WHEN originalConstructionDate < DATE '1900-01-01' THEN 1 ELSE 0 END) AS pre_1900,
        MIN(originalConstructionDate) AS earliest_construction
    FROM {TARGET_TABLE}
""").display()