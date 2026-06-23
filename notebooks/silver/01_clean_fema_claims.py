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
