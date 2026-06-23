# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Clean Synthetic Data (Agents, Customers, Policies)

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType

PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)

print(f"Pipeline run id: {PIPELINE_RUN_ID}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### silver.agents_clean

# COMMAND ----------

df_agents = spark.table("bronze.synthetic_agents_raw")
print(f"bronze.synthetic_agents_raw: {df_agents.count():,}")
df_agents.printSchema()

# COMMAND ----------

df_agents_silver = (
    df_agents
    .withColumn("hire_date", F.col("hire_date").cast(DateType()))
    .withColumn("commission_rate", F.col("commission_rate").cast(DecimalType(5, 4)))
    .withColumn("_silver_ingested_at", F.lit(INGESTED_AT).cast("timestamp"))
    .withColumn("_silver_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
)

(
    df_agents_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.agents_clean")
)

print(f"Wrote silver.agents_clean ({df_agents_silver.count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver.customers_clean

# COMMAND ----------

df_customers = spark.table("bronze.synthetic_customers_raw")
print(f"bronze.synthetic_customers_raw: {df_customers.count():,}")
df_customers.printSchema()

# COMMAND ----------

df_customers_silver = (
    df_customers
    .withColumn("dob", F.col("dob").cast(DateType()))
    .withColumn("_silver_ingested_at", F.lit(INGESTED_AT).cast("timestamp"))
    .withColumn("_silver_pipeline_run_id", F.lit("PIPELINE_RUN_ID"))
)

(
    df_customers_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.customers_clean")
)

print(f"Wrote silver.customers_clean ({df_customers_silver.count():,} rows)")



# COMMAND ----------

# MAGIC %md
# MAGIC ### silver.policies_clean

# COMMAND ----------

df_policies = spark.table("bronze.synthetic_policies_raw")
print(f"bronze.synthetic_policies_raw: {df_policies.count():,} rows")
df_policies.printSchema()

# COMMAND ----------

df_policies_silver = (
    df_policies
    .withColumn("building_coverage", F.col("building_coverage").cast(DecimalType(18, 2)))
    .withColumn("contents_coverage", F.col("contents_coverage").cast(DecimalType(18, 2)))
    .withColumn("deductiable_amount", F.col("deductible_amount").cast(DecimalType(18,2)))
    .withColumn("annual_premium", F.col("annual_premium").cast(DecimalType(18,2)))
)

(
    df_policies_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.policies_clean")
)

print(f"Wrote silver.policies.clean ({df_policies_silver.count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify All Three

# COMMAND ----------

for table in [
    "silver.agents_clean",
    "silver.customers_clean",
    "silver.policies_clean"
]:
    n = spark.table(table).count()
    print(f"{table}: {n:,} rows")