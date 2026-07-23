from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

logger = logging.getLogger(__name__)

# configuration

# databricks notebooks - synched from GitHub via Databricks Git folder
NOTEBOOK_BASE = (
    "/Workspace/Users/halill.eibas@outlook.com/Flood-analytics-pipeline/notebooks"
)

# dbt config for DockerOperator
DBT_IMAGE = "dbt-flood-analytics:1.12.2"
HOST_PROJECT_DIR = "/Users/halil/Desktop/insurance_claim/claims-policy-analytics-pipeline"
HOST_DBT_PROJECT = f"{HOST_PROJECT_DIR}/dbt/flood_analytics"
HOST_DBT_PROFILES = f"{HOST_PROJECT_DIR}/home_dbt"

def task_failure_alert(context: dict) -> None:
    """
    Failure callback - logs structured failure info in Slack/webhook ready shape
    """
    task_instance = context["task_instance"]
    dag_run = context["dag_run"]
    exception = context.get("exception", "unknown")
    
    payload = {
        "dag_id": task_instance.dag_id,
        "task_id": task_instance.task_id,
        "run_id": dag_run.run_id,
        "try_number": task_instance.try_number,
        "max_tries": task_instance.max_tries,
        "log_url": task_instance.log_url,
        "exception": str(exception),
        "execution_date": str(context.get("execution_date")),
    }
    
    logger.error(
        "PIPELINE TASK FAILURE",
        extra={"failure_payload": payload},
    )
    
    logger.error(
        f"[ALERT] {task_instance.dag_id}.{task_instance.task_id} failed "
        f"(try {task_instance.try_number}/{task_instance.max_tries}). "
        f"Log: {task_instance.log_url}"
    )

default_args = {
    "owner": "halil",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": task_failure_alert,
}

# task

def databricks_notebook_task(
    task_id: str, notebook_subpath: str
) -> DatabricksSubmitRunOperator:
    """
    Databricks Serverless notebook task via multi-task Jobs API.
    """
    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id="databricks_default",
        retries=2,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(minutes=15),
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


def dbt_task(task_id: str, command: str) -> DockerOperator:
    """
    dbt task via DockerOperator — runs dbt in an isolated container to avoid
    dependency conflict between dbt-databricks and apache-airflow-providers-databricks
    (both pin different databricks-sql-connector versions).
    """
    return DockerOperator(
        task_id=task_id,
        image=DBT_IMAGE,
        command=command,
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
        mounts=[
            Mount(source=HOST_DBT_PROJECT, target="/usr/app/dbt", type="bind"),
            Mount(source=HOST_DBT_PROFILES, target="/root/.dbt", type="bind"),
        ],
        auto_remove="success",
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

# dag

with DAG(
    dag_id="medallion_full_refresh",
    default_args=default_args,
    description=(
        "End-to-end Flood Analytics pipeline: Bronze ingest → Silver clean/enrich "
        "→ dbt materialize gold tables → dbt test. Two orchestration patterns "
        "united (DatabricksSubmitRunOperator + DockerOperator)."
    ),
    schedule=None,   
    start_date=datetime(2026, 7, 21),
    catchup=False,
    tags=["flood_analytics", "medallion", "canonical"],
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=45)
) as dag:
    
    # bronze layer
    bronze_fema = databricks_notebook_task(
        task_id="bronze_load_fema_claims",
        notebook_subpath="bronze/01_load_fema_claims",
    )
    
    bronze_synthetic = databricks_notebook_task(
        task_id="bronze_load_synthetic",
        notebook_subpath="bronze/02_load_synthetic",
    )

    bronze_reference_tables = databricks_notebook_task(
        task_id="bronze_load_reference_tables",
        notebook_subpath="bronze/03_load_reference_tables",
    )
    
    # silver layer
    
    silver_clean_fema = databricks_notebook_task(
        task_id="silver_clean_fema_claims",
        notebook_subpath="silver/01_clean_fema_claims",
    )

    silver_clean_synthetic = databricks_notebook_task(
        task_id="silver_clean_synthetic",
        notebook_subpath="silver/02_clean_synthetic",
    )

    silver_enrich = databricks_notebook_task(
        task_id="silver_enrich_claims",
        notebook_subpath="silver/03_enrich_claims",
    )

    silver_synthesize_dates = databricks_notebook_task(
        task_id="silver_synthesize_lifecycle_dates",
        notebook_subpath="silver/04_synthesize_lifecycle_dates",
    )
    
    # gold layer via dbt
    
    dbt_run = dbt_task(
        task_id="dbt_run",
        command="dbt run --project-dir /usr/app/dbt --profiles-dir /root/.dbt",
    )

    dbt_test = dbt_task(
        task_id="dbt_test",
        command="dbt test --project-dir /usr/app/dbt --profiles-dir /root/.dbt",
    )
    
    # dependencies
    
    # bronze pairs to cleaned silver
    bronze_fema >> silver_clean_fema
    bronze_synthetic >> silver_clean_synthetic

    # enrichment needs both cleaned silvers
    [silver_clean_fema, silver_clean_synthetic] >> silver_enrich

    # silver chain completes with synthesize_dates
    silver_enrich >> silver_synthesize_dates

    # gold: dbt runs after all silver is complete
    silver_synthesize_dates >> dbt_run >> dbt_test
    