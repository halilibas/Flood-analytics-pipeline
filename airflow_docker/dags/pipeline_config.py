"""
Environment-specific configuration shared by the flood analytics DAGs
"""

import os


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(
            f"{name} is not set. Add it to airflow_docker/.env "
            f"(see .env.example), then run `docker compose up -d` "
            f"to recreate the containers."
        )
    return value


# Root of the Databricks Git folder holding the synced notebooks.
NOTEBOOK_BASE = _required("DATABRICKS_NOTEBOOK_BASE")

# Repo root on the Docker host, used as the dbt bind-mount source.
HOST_PROJECT_DIR = _required("HOST_PROJECT_DIR")
HOST_DBT_PROJECT = f"{HOST_PROJECT_DIR}/dbt/flood_analytics"
HOST_DBT_PROFILES = f"{HOST_PROJECT_DIR}/home_dbt"