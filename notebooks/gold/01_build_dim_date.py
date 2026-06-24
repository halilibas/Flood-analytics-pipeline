# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - dim_date

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType, StringType, BooleanType

TARGET_TABLE = "gold.dim_date"
START_DATE = "1970-01-01"
END_DATE = "2030-12-31"

PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
print(f"Building {TARGET_TABLE} from {START_DATE} to {END_DATE}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate The Calendar

# COMMAND ----------

df_dates = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{START_DATE}'),
        to_date('{END_DATE}'),
        interval 1 day
    )) AS date                                        
""")

print(f"Generated {df_dates.count():,} dates")
df_dates.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Derive All Attributes

# COMMAND ----------

dim_date = (
    df_dates
    # The smart key
    .withColumn("date_key", F.date_format(F.col("date"), "yyyyMMdd").cast(IntegerType()))

    # Calendar parts
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("month_short", F.date_format("date", "MMM"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_of_year", F.dayofyear("date"))
    .withColumn("day_of_week", F.dayofweek("date"))  # 1=Sunday, 7=Saturday
    .withColumn("day_name", F.date_format("date", "EEEE"))
    .withColumn("day_name_short", F.date_format("date", "EEE"))
    .withColumn("week_of_year", F.weekofyear("date"))

    # Weekend flag
    .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))

    # Quarter formatted as e.g. "Q3 2017"
    .withColumn("quarter_year", F.concat(F.lit("Q"), F.col("quarter"), F.lit(" "), F.col("year")))

    # Year-month for monthly rollups
    .withColumn("year_month", F.date_format("date", "yyyy-MM"))

    # Insurance domain: Atlantic hurricane season runs Jun 1 - Nov 30
    .withColumn(
        "hurricane_season_flag",
        (F.col("month") >= 6) & (F.col("month") <= 11)
    )

    # Audit columns
    .withColumn("_dim_built_at", F.lit(INGESTED_AT).cast("timestamp"))
    .withColumn("_dim_pipeline_run_id", F.lit(PIPELINE_RUN_ID))

    # Reorder for readability
    .select(
        "date_key",
        "date",
        "year",
        "quarter",
        "quarter_year",
        "month",
        "month_name",
        "month_short",
        "year_month",
        "day",
        "day_of_year",
        "day_of_week",
        "day_name",
        "day_name_short",
        "week_of_year",
        "is_weekend",
        "hurricane_season_flag",
        "_dim_built_at",
        "_dim_pipeline_run_id",
    )
)

dim_date.show(5)
print(f"Total rows: {dim_date.count():,}")
print(f"Total columns: {len(dim_date.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Gold

# COMMAND ----------

(
    dim_date.write
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

# Hurricane Katrina landfall = August 29 2005
spark.sql(f"""
    SELECT * FROM {TARGET_TABLE}
    WHERE date_key = 20050829                
""").display()

# Verify range
spark.sql(f"""
    SELECT
        MIN(date) AS first_date,
        MAX(date) AS last_date,
        COUNT(*) AS total_days,
        SUM(CASE WHEN hurricane_season_flag THEN 1 ELSE 0 END) AS total_hurricane_days
    FROM {TARGET_TABLE}    
""").display()

# Verify distribution by year
spark.sql(f"""
    SELECT year, COUNT(*) AS days
    FROM {TARGET_TABLE}
    GROUP BY year
    HAVING COUNT(*) NOT IN (365, 366)
""").display()
