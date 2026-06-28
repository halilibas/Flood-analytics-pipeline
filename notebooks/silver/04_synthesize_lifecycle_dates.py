# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Synthesize Claim Lifecycle Dates

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)

claims = spark.table("silver.claims_clean")
print(f"Source rows: {claims.count():,}")
print(f"Source columns: {len(claims.columns)}")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Determine which claims received any payment

# COMMAND ----------

claims_with_payment_flag = claims.withColumn(
    "_has_payment",
    F.coalesce(
        F.col("amountPaidOnBuildingClaim"),
        F.col("amountPaidOnContentsClaim"),
        F.col("amountPaidOnIncreasedCostOfComplianceClaim"),
    ).isNotNull()
)


# COMMAND ----------

# MAGIC %md
# MAGIC # Sanity: how many claims have any payment vs none?
# MAGIC
# MAGIC

# COMMAND ----------

spark.sql("SELECT 1").collect()  
payment_counts = claims_with_payment_flag.groupBy("_has_payment").count().collect()
for row in payment_counts:
    print(f"_has_payment={row['_has_payment']}: {row['count']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deterministic per-claim offsets

# COMMAND ----------

def hash_to_int(col, n_chars=8):
    return F.conv(F.substring(F.md5(col.cast("string")),1, n_chars), 16, 10).cast("long")

# Three independent hash columns by salting differently
h1 = hash_to_int(F.concat(F.col("id"), F.lit("|filed")))
h2 = hash_to_int(F.concat(F.col("id"), F.lit("|paid")))
h3 = hash_to_int(F.concat(F.col("id"), F.lit("|closed")))

# Offset ranges (days)
filed_offet = (h1 % 30) + F.lit(1) # 1-30
paid_offset = (h2 % 84) + F.lit(7) # 7-90
closed_offset = (h3 % 336) + F.lit(30) # 30-365

# Compute dates
claims_with_dates = (
    claims_with_payment_flag
    .withColumn("date_filed",
        F.date_add(F.col("dateOfLoss"), filed_offet.cast("int")))
    .withColumn("date_first_payment",
        F.when(F.col("_has_payment"),
               F.date_add(F.col("date_filed"), paid_offset.cast("int")))
         .otherwise(F.lit(None).cast(DateType())))
    .withColumn("date_closed",
        F.date_add(F.col("date_filed"), closed_offset.cast("int")))
    # Ensure date_closed >= date_first_payment when both exist
    .withColumn("date_closed",
        F.when(
            F.col("date_first_payment").isNotNull() & (F.col("date_closed") < F.col("date_first_payment")),
            F.date_add(F.col("date_first_payment"), 1)
        ).otherwise(F.col("date_closed"))
    )
    .drop("_has_payment")
)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanity check the derived dates

# COMMAND ----------

spark.sql("SELECT 1 ").collect()
claims_with_dates.select(
    "id", "dateOfLoss", "date_filed", "date_first_payment", "date_closed"
).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distribution Checks

# COMMAND ----------

print("date_filed offset distribution (days from loss): ")
claims_with_dates.selectExpr("datediff(date_filed, dateOfLoss) AS days_loss_to_filed") \
    .summary("min", "25%", "50%", "75%", "max").display()

print("date_closed offset distribution (days from filed): ")
claims_with_dates.selectExpr("datediff(date_closed, date_filed) AS days_filed_to_closed") \
    .summary("min", "25%", "50%", "75%", "max").display()

print("date_first_payment NULL rate: ")
claims_with_dates.selectExpr(
    "SUM(CASE WHEN date_first_payment IS NULL THEN 1 ELSE 0 END) AS n_null",
    "SUM(CASE WHEN date_first_payment IS NOT NULL THEN 1 ELSE 0 END) AS n_present"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Determinism check: pick 5 specific claim IDs

# COMMAND ----------

sample_ids = ["d218ee83-cb79-4739-a652-0844cd6016f6"]  
sample = claims_with_dates.filter(F.col("id").isin(sample_ids))
sample.select("id", "date_filed", "date_first_payment", "date_closed").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write back to silver.claims_clean (overwrite)

# COMMAND ----------

(
    claims_with_dates.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.claims_clean")
)

print(f"Updated silver.claims_clean with lifecycle dates")
print(f"Total columns now: {len(claims_with_dates.columns)}")
print(f"Row count: {claims_with_dates.count():,}")