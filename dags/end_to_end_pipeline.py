"""
End-to-end Airflow DAG orchestrating the complete ELT pipeline:
1. Bronze (Raw Ingestion):
   - dbt seed: Ingest company and sector CSVs (raw_companies, raw_sectors)
   - Python: Ingest EMTA revenue and employee count data (raw_emta_data)
2. Silver (Transformation / Staging & Intermediate):
   - dbt run staging_models (views in public_staging)
   - dbt run intermediate_models (views in public_staging)
3. Gold (Analytical Dimensional Model / Marts):
   - dbt run mart_models (tables in public_dwh: star schema)
4. Quality & Governance:
   - dbt test (runs schema and data tests)
"""

from __future__ import annotations

import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

DBT_PROJECT_DIR = "/dbt"
DBT_PROFILES_DIR = "/dbt"
DBT_FLAGS = f"--no-use-colors --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}


with DAG(
    dag_id="end_to_end_pipeline",
    description="Full data pipeline: Ingestion (Bronze) -> dbt Staging (Silver) -> dbt Marts (Gold) -> dbt Test",
    schedule=None,  # Trigger manually or via schedule as needed
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    params={
        "run_tests": Param(True, type="boolean", description="Whether to execute dbt tests after building the marts"),
        "full_refresh": Param(False, type="boolean", description="Whether to run dbt with --full-refresh flag"),
    },
    tags=["dbt", "transform", "silver", "gold", "quality"],
) as dag:

    # -------------------------------------------------------------
    # 1. SILVER LAYER: Staging & Intermediate Models (dbt)
    # -------------------------------------------------------------
    dbt_staging = BashOperator(
        task_id="silver_dbt_staging_models",
        bash_command=f"dbt run {DBT_FLAGS} --selector staging_models",
        doc_md="Creates staging views in public_staging (stg_companies, stg_emta, stg_sectors, stg_procurements).",
    )

    dbt_intermediate = BashOperator(
        task_id="silver_dbt_intermediate_models",
        bash_command=f"dbt run {DBT_FLAGS} --selector intermediate_models",
        doc_md="Creates intermediate models (int_company_sector_history).",
    )

    # -------------------------------------------------------------
    # 2. GOLD LAYER: Dimensional Star Schema Marts (dbt)
    # -------------------------------------------------------------
    dbt_marts = BashOperator(
        task_id="gold_dbt_mart_models",
        bash_command=(
            f"if [ '{{{{ params.full_refresh }}}}' = 'True' ]; then "
            f"dbt run {DBT_FLAGS} --selector mart_models --full-refresh; "
            f"else "
            f"dbt run {DBT_FLAGS} --selector mart_models; "
            f"fi"
        ),
        doc_md="Builds fact and dimension tables in public_dwh.",
    )

    # -------------------------------------------------------------
    # 3. DATA QUALITY & GOVERNANCE: dbt tests
    # -------------------------------------------------------------
    dbt_test = BashOperator(
        task_id="quality_dbt_tests",
        bash_command=(
            f"if [ '{{{{ params.run_tests }}}}' = 'True' ]; then "
            f"dbt test {DBT_FLAGS}; "
            f"else "
            f"echo 'Skipping dbt tests as run_tests is False'; "
            f"fi"
        ),
        doc_md="Runs all dbt data tests to validate integrity of the gold layer.",
    )

    # -------------------------------------------------------------
    # Pipeline Dependencies (Start to Finish)
    # -------------------------------------------------------------

    dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_test
