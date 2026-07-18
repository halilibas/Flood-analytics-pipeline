"""
hello_databricks.py

First working DAG for the flood analytics pipeline project.
Verifies that Airflow can talk to Databricks via the configured connection.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

default_args = {
    "owner": "halil",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def print_start_message():
    print("Airflow DAG started — hello from Docker Airflow!")
    return "ok"


with DAG(
    dag_id="hello_databricks",
    default_args=default_args,
    description="First test DAG — proves Airflow-Docker can run Python + Databricks SQL",
    schedule=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["flood_analytics", "test"],
) as dag:

    print_start = PythonOperator(
        task_id="print_start",
        python_callable=print_start_message,
    )

    query_fact_claims = DatabricksSqlOperator(
        task_id="query_fact_claims",
        databricks_conn_id="databricks_default",
        sql="SELECT COUNT(*) AS n_claims FROM workspace.gold.fact_claims",
        http_path="sql/1.0/warehouses/35566e6924938e28",
    )

    print_start >> query_fact_claims
