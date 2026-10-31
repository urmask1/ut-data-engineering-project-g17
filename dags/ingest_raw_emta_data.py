"""
Airflow DAG that orchestrates fetching and upserting revenue and employee count data from EMTA.

The data will be written to table public_raw.raw_emta_data and will use older data (years 2022-2024) if it is first
run and table is empty.

If table is not empty (meaning older data has already been ingested) then only most recent data
(current and previous year) will be fetched. whatever we get will be upserted (any new pieces of data that EMTA has
added to their CSV file will be upserted).

Pipeline shape:
    fetch_old_data -> fetch_new_data

Schedule: Set to run once per day
If need to trigger manually run it from Airflow UI or from outside:
    docker compose exec airflow-scheduler airflow dags trigger ingest_raw_emta_data
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor

import pendulum
import pandas as pd
from psycopg2.extras import execute_values
from datetime import timedelta

CONN_ID = "project_db"

OLD_DATA="https://ncfailid.emta.ee/s/bCszrta8THHA9xn/download/tasutud_maksud_varasemad_aastad.csv"
RECENT_DATA="https://ncfailid.emta.ee/s/DFHQjB2Rsq3CK7p/download/tasutud_maksud_kaesolev_aasta.csv"

default_args = {
    "owner": "airflow",
    # The submission step can fail on a transient database problem, so every
    # task retries a few times before the run is marked failed.
    "retries": 3,
    "retry_delay": timedelta(seconds=5),
}

def ingest_old_data(**context):
    """
    This task will get old data (revenue and employee count) from EMTA and will write it to db.
    But will do so only if the raw_emta_data table is empty. Otherwise, only the recent data should be fetched.
    """

    # First check if table is empty or not. Only fetch older data if table is empty
    table_empty = is_table_empty()

    if not table_empty:
        # table is not empty, move on to fetching recent data
        return

    # table is empty, start with fetching and inserting old data

    # Query data from EMTA
    fetch_and_upsert(data_url=OLD_DATA)

def is_table_empty():
    """
    This function will do a check to see if raw_emta_data is empty or not
    :return:
    """
    pg = PostgresHook(postgres_conn_id=CONN_ID)

    # Fast check: returns None if table is empty, or (1,) if at least one row exists
    result = pg.get_first("SELECT 1 FROM public_raw.raw_emta_data LIMIT 1;")

    if result is None:
        print("Table raw_emta_data is empty. Need to fetch old emta data")
        return True

    print("Table raw_emta_data contains data. No need to fetch old emta data")
    return False


def ingest_new_data(**context):
    """
    This task will get data (revenue and employee count) from EMTA and will write it to db
    """
    fetch_and_upsert(data_url=RECENT_DATA)


def fetch_and_upsert(data_url):
    """
    Fetch data for which the data url has been specified and upsert to public_raw.raw_emta_data
    :param data_url:
    :return:
    """

    # Load csv directly from URL using pandas
    df = pd.read_csv(data_url, encoding="utf-8")

    # Create mapping of quarterly data that needs to be writtent on separate rows
    quarter_mapping = {
        1: ("Käive I kv", "Töötajate arv I kv"),
        2: ("Käive II kv", "Töötajate arv II kv"),
        3: ("Käive III kv", "Töötajate arv III kv"),
        4: ("Käive IV kv", "Töötajate arv IV kv"),
    }

    # Reshape wide dataset into quarterly dataframes per the quarterly mapping
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

    # Add the quarterly dataframes together into one
    long_df = pd.concat(quarter_dfs, ignore_index=True)

    # For FIEs there seems to be two ID values under Registrikood (formatted as "<code>; <code>"). We want to drop
    # these as the registration code needs to be integer for our table, and we also don't care about FIEs.
    # errors=coerce will make the non-numeric values into NaN which we can simply drop as the next step.
    long_df["company_registration_code"] = pd.to_numeric(
        long_df["company_registration_code"], errors="coerce"
    )

    # Filter out rows where any of company_registration_code OR revenue OR employee_count is NaN
    long_df = long_df.dropna(subset=["company_registration_code", "revenue", "employee_count"])

    # Cast company_registration_code to integer, otherwise the id value will be with trailing .0 (earlier to_numeric
    # converts value to float) - 'Int64' (Pandas integer type) or standard int
    long_df["company_registration_code"] = long_df[
        "company_registration_code"
    ].astype("Int64")

    # Create unique ID: year_quarter_registrikood (e.g. "2026_1_10000024")
    long_df["id"] = (
            long_df["year"].astype(str)
            + "_"
            + long_df["quarter"].astype(str)
            + "_"
            + long_df["company_registration_code"].astype(str)
    )

    # Reorder columns to match DB schema
    db_columns = [
        "id",
        "year",
        "quarter",
        "company_registration_code",
        "company_name",
        "field_of_activity",
        "revenue",
        "employee_count",
    ]
    long_df = long_df[db_columns]

    # Convert DataFrame rows into tuples
    data_tuples = [tuple(x) for x in long_df.to_numpy()]

    # Prepare SQL UPSERT query
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

    # Create postgres hook
    pg = PostgresHook(postgres_conn_id=CONN_ID)

    # Get raw psycopg2 connection provided by PostgresHook
    conn = pg.get_conn()

    # Execute the query writing all the data in tuples into db, 10000 rows at a time for good performance
    with conn.cursor() as cursor:
        execute_values(cursor, upsert_query, data_tuples, page_size=10000)

    conn.commit()
    conn.close()

    print(f"Successfully upserted {len(data_tuples)} records into Postgres.")

with DAG(
    dag_id="ingest_raw_emta_data",
    description="Query emta data file and insert into DB",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # Scheduled: once per day: at 00:00
    schedule="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ingest", "raw", "emta"],
) as dag:

    t_fetch_old_data = PythonOperator(
        task_id="fetch_old_data",
        python_callable=ingest_old_data
    )

    t_fetch_new_data = PythonOperator(
        task_id="fetch_new_data",
        python_callable=ingest_new_data,
    )

    t_fetch_old_data >> t_fetch_new_data
