# Databricks notebook source
# MAGIC %md
# MAGIC ## Read the CSV

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/warmup/warmup_v1/2012_SAT_Results_20260611.csv", header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect: Columns, types and rows

# COMMAND ----------

df.printSchema()


# COMMAND ----------

df.show(5)

# COMMAND ----------

print(f"Row count: {df.count()}")
print(f"Columns: {df.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## First 5 rows of School name and AVG Math Score

# COMMAND ----------

subset = df.select("SCHOOL NAME", "`SAT Math Avg. Score`")
subset.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Filter column

# COMMAND ----------

df.select("`SAT MATH Avg. Score`").distinct().show(20, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Computed Column

# COMMAND ----------

df.createOrReplaceTempView("schools")

spark.sql("""
SELECT *
FROM schools
WHERE try_cast(`SAT Math Avg. Score` AS INT) > 690
""").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Group and Aggregate

# COMMAND ----------

from pyspark.sql.functions import count

agg = df.groupBy("Num of SAT Test Takers") \
    .agg(count("*").alias("schools")) 



# COMMAND ----------

# MAGIC %md
# MAGIC ##Write the aggregated result back out as parquet

# COMMAND ----------

agg.write.mode("overwrite").parquet(
    "/Volumes/workspace/warmup/warmup_v1/warmup_aggregates"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read the parguet file back

# COMMAND ----------

agg_reread = spark.read.parquet("/Volumes/workspace/warmup/warmup_v1/warmup_aggregates")
agg_reread.show(5)