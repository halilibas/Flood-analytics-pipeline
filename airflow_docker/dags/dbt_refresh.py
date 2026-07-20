"""

Runs dbt run followed by dbt test against the flood_analytics project.
Uses DockerOperator to invoke dbt in its own isolated container,
sidestepping the dependency conflict between dbt-databricks and
apache-airflow-providers-databricks (both pin different versions of
databricks-sql-connector).

Architecture:
    - Airflow worker (dependency-conflict-free) invokes DockerOperator
    - DockerOperator spins up a dbt-flood-analytics:1.12.2 container
    - dbt container bind-mounts:
        - dbt project source (from host dbt/flood_analytics)
        - profiles.yml (from host home_dbt/)
    - Container runs dbt run or dbt test, exits, is cleaned up
    - Airflow captures stdout/stderr in the task log

Task chain:
    dbt_run  →  dbt_test
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


# Configuration

DBT_IMAGE = "dbt-flood-analytics:1.12.2"

# host paths ,DockerOperator needs these to bind-mount into the dbt container
HOST_PROJECT_DIR = "/Users/halil/Desktop/insurance_claim/claims-policy-analytics-pipeline"
HOST_DBT_PROJECT = f"{HOST_PROJECT_DIR}/dbt/flood_analytics"
HOST_DBT_PROFILES = f"{HOST_PROJECT_DIR}/home_dbt"


default_args = {
    "owner": "halil",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


# DAG
with DAG(
    dag_id="dbt_refresh",
    default_args=default_args,
    description="Rebuild + test flood_analytics dbt project via isolated Docker container",
    schedule=None,
    start_date=datetime(2026, 7, 15),
    catchup=False,
    tags=["flood_analytics", "dbt", "gold"],
) as dag:

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image=DBT_IMAGE,
        command="dbt run --project-dir /usr/app/dbt --profiles-dir /root/.dbt",
        mounts=[
            Mount(source=HOST_DBT_PROJECT, target="/usr/app/dbt", type="bind"),
            Mount(source=HOST_DBT_PROFILES, target="/root/.dbt", type="bind"),
        ],
        auto_remove="success",     
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    dbt_test = DockerOperator(
        task_id="dbt_test",
        image=DBT_IMAGE,
        command="dbt test --project-dir /usr/app/dbt --profiles-dir /root/.dbt",
        mounts=[
            Mount(source=HOST_DBT_PROJECT, target="/usr/app/dbt", type="bind"),
            Mount(source=HOST_DBT_PROFILES, target="/root/.dbt", type="bind"),
        ],
        auto_remove="success",
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    dbt_run >> dbt_test