# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze -  Synthetic Data Load

# COMMAND ----------

SOURCES = {
    "bronze.synthetic_agents_raw" : "/Volumes/workspace/filestore/raw/agents.parquet",

    "bronze.synthetic_customers_raw": "/Volumes/workspace/filestore/raw/customers.parquet",

    "bronze.synthetic_policies_raw": "/Volumes/workspace/filestore/raw/policies.parquet"
}

for target, source in SOURCES.items():
    df = spark.read.parquet(source)
    print(f"\n{target}: read {df.count():,} rows from {source}")
    (
        df.write
        .mode("overwrite")  
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )
    print(f" -> wrote {target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification

# COMMAND ----------

spark.sql("SELECT COUNT(*) AS n FROM bronze.synthetic_agents_raw").display()
spark.sql("SELECT COUNT(*) AS n FROM bronze.synthetic_customers_raw").display()
spark.sql("SELECT COUNT(*) AS n FROM bronze.synthetic_policies_raw").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample each

# COMMAND ----------

spark.sql("SELECT * FROM bronze.synthetic_agents_raw LIMIT 5").display()
spark.sql("SELECT * FROM bronze.synthetic_customers_raw LIMIT 5").display()
spark.sql("SELECT * FROM bronze.synthetic_policies_raw LIMIT 5").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanity Check: synthetic policies can be joined to FEMA claims

# COMMAND ----------

spark.sql("""
    SELECT
        COUNT(*) AS matched_claims,
        COUNT(DISTINCT p.policy_number) AS distinct_policies
    FROM bronze.fema_claims_raw AS f
    INNER JOIN bronze.synthetic_policies_raw AS p
        ON f.id = p.fema_claim_id   

""").display()