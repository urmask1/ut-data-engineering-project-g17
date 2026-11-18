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
import pandas as pd
from datetime import timedelta
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param

CONN_ID = "project_db"

OLD_DATA = "https://ncfailid.emta.ee/s/bCszrta8THHA9xn/download/tasutud_maksud_varasemad_aastad.csv"
RECENT_DATA = "https://ncfailid.emta.ee/s/DFHQjB2Rsq3CK7p/download/tasutud_maksud_kaesolev_aasta.csv"

DBT_PROJECT_DIR = "/dbt"
DBT_PROFILES_DIR = "/dbt"
DBT_FLAGS = f"--no-use-colors --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}


# Checks if raw_emta_data table has any rows.
def is_table_empty():
    pg = PostgresHook(postgres_conn_id=CONN_ID)
    result = pg.get_first("SELECT 1 FROM public_raw.raw_emta_data LIMIT 1;")
    return result is None


# Downloads EMTA CSV and upserts records into public_raw.raw_emta_data.
def fetch_and_upsert(data_url: str):
    df = pd.read_csv(data_url, encoding="utf-8")
    quarter_mapping = {
        1: ("Käive I kv", "Töötajate arv I kv"),
        2: ("Käive II kv", "Töötajate arv II kv"),
        3: ("Käive III kv", "Töötajate arv III kv"),
        4: ("Käive IV kv", "Töötajate arv IV kv"),
    }

    quarter_dfs = []
    for q_num, (rev_col, emp_col) in quarter_mapping.items():
        q_df = pd.DataFrame({
            "year": df["Aasta"],
            "quarter": q_num,
            "company_registration_code": df["Registrikood"],
            "company_name": df["Nimi"],
            "field_of_activity": df["Tegevusala"],
            "revenue": df[rev_col],
            "employee_count": df[emp_col],
        })
        quarter_dfs.append(q_df)

    long_df = pd.concat(quarter_dfs, ignore_index=True)
    long_df["company_registration_code"] = pd.to_numeric(
        long_df["company_registration_code"], errors="coerce"
    )
    long_df = long_df.dropna(subset=["company_registration_code", "revenue", "employee_count"])
    long_df["company_registration_code"] = long_df["company_registration_code"].astype("Int64")
    long_df["id"] = (
        long_df["year"].astype(str)
        + "_"
        + long_df["quarter"].astype(str)
        + "_"
        + long_df["company_registration_code"].astype(str)
    )

    db_columns = [
        "id", "year", "quarter", "company_registration_code",
        "company_name", "field_of_activity", "revenue", "employee_count"
    ]
    long_df = long_df[db_columns]
    data_tuples = [tuple(x) for x in long_df.to_numpy()]

    upsert_query = """
        INSERT INTO public_raw.raw_emta_data (
            id, year, quarter, company_registration_code, company_name,
            field_of_activity, revenue, employee_count
        ) VALUES %s ON CONFLICT (id) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            field_of_activity = EXCLUDED.field_of_activity,
            revenue = EXCLUDED.revenue,
            employee_count = EXCLUDED.employee_count;
    """

    pg = PostgresHook(postgres_conn_id=CONN_ID)
    conn = pg.get_conn()
    with conn.cursor() as cursor:
        execute_values(cursor, upsert_query, data_tuples, page_size=10000)
    conn.commit()
    conn.close()
    print(f"Successfully upserted {len(data_tuples)} records into Postgres.")


# Fetches older data only if table is currently empty.
def ingest_old_emta_data(**context):
    if not is_table_empty():
        print("Table raw_emta_data is not empty. Skipping old data fetch.")
        return
    print("Table raw_emta_data is empty. Fetching historical EMTA data.")
    fetch_and_upsert(data_url=OLD_DATA)


# Fetches current and recent year data from EMTA.
def ingest_recent_emta_data(**context):
    print("Fetching recent EMTA data.")
    fetch_and_upsert(data_url=RECENT_DATA)


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
    tags=["elt", "dbt", "ingest", "gold", "end-to-end"],
) as dag:

    # -------------------------------------------------------------
    # 1. BRONZE LAYER: Raw Data Ingestion
    # -------------------------------------------------------------
    dbt_seed = BashOperator(
        task_id="bronze_seed_companies_sectors",
        bash_command=f"dbt seed {DBT_FLAGS}",
        doc_md="Seeds raw_companies and raw_sectors into public_raw schema.",
    )

    ingest_emta_old = PythonOperator(
        task_id="bronze_ingest_emta_old_data",
        python_callable=ingest_old_emta_data,
        doc_md="Ingests historical EMTA data if raw_emta_data is empty.",
    )

    ingest_emta_recent = PythonOperator(
        task_id="bronze_ingest_emta_recent_data",
        python_callable=ingest_recent_emta_data,
        doc_md="Ingests recent/current EMTA quarterly revenue and employee count.",
    )

    # -------------------------------------------------------------
    # 2. SILVER LAYER: Staging & Intermediate Models (dbt)
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
    # 3. GOLD LAYER: Dimensional Star Schema Marts (dbt)
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
    # 4. DATA QUALITY & GOVERNANCE: dbt tests
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
    ingest_emta_old >> ingest_emta_recent

    [dbt_seed, ingest_emta_recent] >> dbt_staging
    dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_test
