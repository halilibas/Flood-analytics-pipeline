# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - FEMA Reference Tables

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4
from pyspark.sql import functions as F

PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)

REFERENCES = {
    "bronze.ref_occupancy_type":         "/Volumes/workspace/filestore/raw/occupancy_type_lookup.csv",
    "bronze.ref_cause_of_damage":        "/Volumes/workspace/filestore/raw/cause_of_damage_lookup.csv",
    "bronze.ref_location_of_contents":   "/Volumes/workspace/filestore/raw/location_of_contents_lookup.csv",
    "bronze.ref_building_description":   "/Volumes/workspace/filestore/raw/building_description_lookup.csv",
    "bronze.ref_flood_zone":             "/Volumes/workspace/filestore/raw/flood_zone_lookup.csv",
}

for table_name, path in REFERENCES.items():
    df = (
        spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(path)
            .withColumn("_ingested_at", F.lit(INGESTED_AT).cast("timestamp"))
            .withColumn("_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
    )
    (
        df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
    )

    n = spark.table(table_name).count()
    print(f"{table_name}: {n} rows")


spark.sql("SELECT * FROM bronze.ref_occupancy_type ORDER BY occupancy_type_code").display()
spark.sql("SELECT * FROM bronze.ref_cause_of_damage ORDER BY cause_of_damage_code").display()
spark.sql("SELECT * FROM bronze.ref_location_of_contents ORDER BY location_of_contents_code").display()
spark.sql("SELECT * FROM bronze.ref_building_description ORDER BY building_description_code").display()
spark.sql("SELECT * FROM bronze.ref_flood_zone ORDER BY flood_zone_code").display()


