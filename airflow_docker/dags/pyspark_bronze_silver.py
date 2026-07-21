"""
Orchestrates Bronze + Silver PySpark notebooks via DatabricksSubmitRunOperator.
Uses the Databricks multi-task Jobs API (2.1) with `tasks` array for Serverless
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
DatabricksSubmitRunOperator,
)



# configuration
NOTEBOOK_BASE = (
    "/Workspace/Users/halill.eibas@outlook.com/Flood-analytics-pipeline/notebooks"
)


default_args = {
    "owner": "halil",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def notebook_task(task_id: str, notebook_subpath: str) -> DatabricksSubmitRunOperator:
    """
    factory for DatabricksSubmitRunOperator tasks using the multi task API
    """
    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id="databricks_default",
        tasks=[
            {
                "task_key": task_id,
                "notebook_task": {
                    "notebook_path": f"{NOTEBOOK_BASE}/{notebook_subpath}",
                    "source": "WORKSPACE",
                },
            }
        ],
    )


# dag
with DAG(
    dag_id="pyspark_bronze_silver",
    default_args=default_args,
    description="Bronze ingest + Silver clean/enrich PySpark notebooks via Databricks Serverless jobs",
    schedule=None,
    start_date=datetime(2026, 7, 15),
    catchup=False,
    tags=["insurance_claims", "pyspark", "bronze", "silver"],
) as dag:

    # bronze (ingest raw sources)
    bronze_fema = notebook_task(
        task_id="bronze_load_fema_claims",
        notebook_subpath="bronze/01_load_fema_claims",
    )

    bronze_synthetic = notebook_task(
        task_id="bronze_load_synthetic",
        notebook_subpath="bronze/02_load_synthetic",
    )

    bronze_reference_tables = notebook_task(
        task_id="bronze_load_reference_tables",
        notebook_subpath="bronze/03_load_reference_tables",
    )

    # silver (clean, enrich, synthesize dates)
    silver_clean_fema = notebook_task(
        task_id="silver_clean_fema_claims",
        notebook_subpath="silver/01_clean_fema_claims",
    )

    silver_clean_synthetic = notebook_task(
        task_id="silver_clean_synthetic",
        notebook_subpath="silver/02_clean_synthetic",
    )

    silver_enrich = notebook_task(
        task_id="silver_enrich_claims",
        notebook_subpath="silver/03_enrich_claims",
    )

    silver_synthesize_dates = notebook_task(
        task_id="silver_synthesize_lifecycle_dates",
        notebook_subpath="silver/04_synthesize_lifecycle_dates",
    )

    # Dependencies
    bronze_fema >> silver_clean_fema
    bronze_synthetic >> silver_clean_synthetic
    [silver_clean_fema, silver_clean_synthetic] >> silver_enrich >> silver_synthesize_dates
  