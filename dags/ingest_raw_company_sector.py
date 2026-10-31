"""
Airflow DAG that orchestrates ingesting company and sector data from CSV files on disk.

The data will be written to tables public_raw.raw_companies, public_raw.raw_sectors

Pipeline shape:
    dbt_seed

dbt_seed runs a dbt CLI command via BashOperator.  dbt-core and dbt-postgres
are pre-installed in the Airflow image (airflow.Dockerfile).  The dbt project
is mounted at /dbt inside the scheduler container.

Schedule: manual trigger only (schedule=None).

Run it from the Airflow UI or:
    docker compose exec airflow-scheduler airflow dags trigger ingest_raw_company_sector
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/dbt"
DBT_PROFILES_DIR = "/dbt"

# Common flags added to every dbt subcommand.
# In dbt 1.8+, --project-dir and --profiles-dir are subcommand flags
# (they follow the subcommand, not dbt itself).
DBT_FLAGS = (
    f"--no-use-colors "
    f"--project-dir {DBT_PROJECT_DIR} "
    f"--profiles-dir {DBT_PROFILES_DIR}"
)

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="ingest_raw_company_sector",
    description="Ingest raw data, CSV files from disk and data via queries over internet",
    schedule=None,   # trigger manually
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "ingest", "raw"],
) as dag:

    # ------------------------------------------------------------------
    # Step 1: Load companies and sectors seed CSVs into the raw schema
    # ------------------------------------------------------------------
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"dbt seed {DBT_FLAGS}",
        doc_md="""
        Loads the CSV files from dbt_project/seeds/ into the `public_raw` schema
        of retail-db:
          - public_raw.raw_companies
          - public_raw.raw_sectors
        """,
    )

    # Define execution order
    dbt_seed
