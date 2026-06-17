# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - Fema NFIP Claims Raw Load

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, TimestampType, DateType
)

SOURCE_PATH = "/Volumes/workspace/filestore/raw/FimaNfipClaimsV2.csv"
TARGET_TABLE = "bronze.fema_claims_raw"
PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)

print(f"Pipeline run id: {PIPELINE_RUN_ID}")
print(f"Ingestion timestamp: {INGESTED_AT.isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explicit schema for FEMA NFIP claims
# MAGIC ### Convention: read everything as StringType in bronze; type casting is silver's job.

# COMMAND ----------

fema_schema = StructType([
    StructField("agricultureStructureIndicator", StringType(), nullable=True),
    StructField("asOfDate", StringType(), nullable=True),
    StructField("basementEnclosureCrawlspaceType", StringType(), nullable=True),
    StructField("policyCount", StringType(), nullable=True),
    StructField("crsClassificationCode", StringType(), nullable=True),
    StructField("dateOfLoss", StringType(), nullable=True),
    StructField("elevatedBuildingIndicator", StringType(), nullable=True),
    StructField("elevationCertificateIndicator", StringType(), nullable=True),
    StructField("elevationDifference", StringType(), nullable=True),
    StructField("baseFloodElevation", StringType(), nullable=True),
    StructField("ratedFloodZone", StringType(), nullable=True),
    StructField("houseWorship", StringType(), nullable=True),
    StructField("locationOfContents", StringType(), nullable=True),
    StructField("lowestAdjacentGrade", StringType(), nullable=True),
    StructField("lowestFloorElevation", StringType(), nullable=True),
    StructField("numberOfFloorsInTheInsuredBuilding", StringType(), nullable=True),
    StructField("nonProfitIndicator", StringType(), nullable=True),
    StructField("obstructionType", StringType(), nullable=True),
    StructField("occupancyType", StringType(), nullable=True),
    StructField("originalConstructionDate", StringType(), nullable=True),
    StructField("originalNBDate", StringType(), nullable=True),
    StructField("amountPaidOnBuildingClaim", StringType(), nullable=True),
    StructField("amountPaidOnContentsClaim", StringType(), nullable=True),
    StructField("amountPaidOnIncreasedCostOfComplianceClaim", StringType(), nullable=True),
    StructField("postFIRMConstructionIndicator", StringType(), nullable=True),
    StructField("rateMethod", StringType(), nullable=True),
    StructField("smallBusinessIndicatorBuilding", StringType(), nullable=True),
    StructField("totalBuildingInsuranceCoverage", StringType(), nullable=True),
    StructField("totalContentsInsuranceCoverage", StringType(), nullable=True),
    StructField("yearOfLoss", StringType(), nullable=True),
    StructField("primaryResidenceIndicator", StringType(), nullable=True),
    StructField("buildingDamageAmount", StringType(), nullable=True),
    StructField("buildingDeductibleCode", StringType(), nullable=True),
    StructField("netBuildingPaymentAmount", StringType(), nullable=True),
    StructField("buildingPropertyValue", StringType(), nullable=True),
    StructField("causeOfDamage", StringType(), nullable=True),
    StructField("condominiumCoverageTypeCode", StringType(), nullable=True),
    StructField("contentsDamageAmount", StringType(), nullable=True),
    StructField("contentsDeductibleCode", StringType(), nullable=True),
    StructField("netContentsPaymentAmount", StringType(), nullable=True),
    StructField("contentsPropertyValue", StringType(), nullable=True),
    StructField("disasterAssistanceCoverageRequired", StringType(), nullable=True),
    StructField("eventDesignationNumber", StringType(), nullable=True),
    StructField("ficoNumber", StringType(), nullable=True),
    StructField("floodCharacteristicsIndicator", StringType(), nullable=True),
    StructField("floodWaterDuration", StringType(), nullable=True),
    StructField("floodproofedIndicator", StringType(), nullable=True),
    StructField("floodEvent", StringType(), nullable=True),
    StructField("iccCoverage", StringType(), nullable=True),
    StructField("netIccPaymentAmount", StringType(), nullable=True),
    StructField("nfipRatedCommunityNumber", StringType(), nullable=True),
    StructField("nfipCommunityNumberCurrent", StringType(), nullable=True),
    StructField("nfipCommunityName", StringType(), nullable=True),
    StructField("nonPaymentReasonContents", StringType(), nullable=True),
    StructField("nonPaymentReasonBuilding", StringType(), nullable=True),
    StructField("numberOfUnits", StringType(), nullable=True),
    StructField("buildingReplacementCost", StringType(), nullable=True),
    StructField("contentsReplacementCost", StringType(), nullable=True),
    StructField("replacementCostBasis", StringType(), nullable=True),
    StructField("stateOwnedIndicator", StringType(), nullable=True),
    StructField("waterDepth", StringType(), nullable=True),
    StructField("floodZoneCurrent", StringType(), nullable=True),
    StructField("buildingDescriptionCode", StringType(), nullable=True),
    StructField("rentalPropertyIndicator", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("reportedCity", StringType(), nullable=True),
    StructField("reportedZipCode", StringType(), nullable=True),
    StructField("countyCode", StringType(), nullable=True),
    StructField("censusTract", StringType(), nullable=True),
    StructField("censusBlockGroupFips", StringType(), nullable=True),
    StructField("latitude", StringType(), nullable=True),
    StructField("longitude", StringType(), nullable=True),
    StructField("id", StringType(), nullable=False),  
])

print(f"Schema defined with {len(fema_schema.fields)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the CSV

# COMMAND ----------

df_raw = (
    spark.read
    .schema(fema_schema)
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .csv(SOURCE_PATH)
)

print(f"Rows read: {df_raw.count():,}")
df_raw.printSchema()
df_raw.limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add audit columns

# COMMAND ----------

df_with_audit = (
    df_raw
    .withColumn("_ingested_at", F.lit(INGESTED_AT).cast(TimestampType()))
    .withColumn("_source_file", F.lit(SOURCE_PATH))
    .withColumn("_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
)

df_with_audit.select("_ingested_at", "_source_file", "_pipeline_run_id", "id").limit(3).display()
print(f"Total columns after audit: {len(df_with_audit.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Delta
# MAGIC

# COMMAND ----------

(
    df_with_audit.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Wrote table: {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Row Count

# COMMAND ----------

#Row count check
source_count = spark.read.schema(fema_schema).option("header", "true").csv(SOURCE_PATH).count()
target_count = spark.table(TARGET_TABLE).count()

print(f"Source CSV rows:   {source_count:,}")
print(f"Bronze table rows: {target_count:,}")
assert source_count == target_count, f"Row count mismatch: {source_count} vs {target_count}"
print("Row count verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanity Queries

# COMMAND ----------

#Schema check
print("Table schema: ")
spark.table(TARGET_TABLE).printSchema()

#Date Range Check
print("Date range: ")
spark.sql(f"""
    SELECT
        MIN(dateOfLoss) AS earliest_loss,
        MAX(dateOfLoss) AS latest_loss,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT id) AS distinct_ids
    FROM {TARGET_TABLE}
""").display()

#Top states check
print("Top 10 states: ")
spark.sql(f"""
    SELECT state, COUNT(*) AS claim_count
    FROM {TARGET_TABLE}
    GROUP BY state
    ORDER BY claim_count DESC
    LIMIT 10
""").display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta history check

# COMMAND ----------

### Look at the transcation log to verify delta is working as expected
spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC