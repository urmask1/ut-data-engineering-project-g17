"""
Airflow DAG that orchestrates fetching and upserting procurement contracts data from
Estonia's national procurement registry.

The data will be written to table public_raw.raw_procurements_data and will include procurement
contracts starting from 2023-06.

If the table is not empty (meaning some raw data has already been ingested), then the script will first
find the year/month of the most recent record. That will be the start year/month for fetching data.

Pipeline shape:
    fetch_procurement_data

Schedule: Set to run once per day
If need to trigger manually run it from Airflow UI or from outside:
    docker compose exec airflow-scheduler airflow dags trigger ingest_raw_procurement_contracts
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor

import requests
import pendulum
import pandas as pd
import xml.etree.ElementTree as ET
from psycopg2.extras import execute_values
from datetime import timedelta, date

CONN_ID = "project_db"

DEFAULT_YEAR = 2023
DEFAULT_MONTH = 6

BASE_URL = (
    "https://riigihanked.riik.ee:443/rhr/api/public/v1/opendata/notice_award"
    "/{year}/month/{month}/xml"
)

# Namespace map used in the XML
NAMESPACES = {
    "can": "urn:oasis:names:specification:ubl:schema:xsd:ContractAwardNotice-2",
    "cac": "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
    "cbc": "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
    "efac": "http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1",
    "efbc": "http://data.europa.eu/p27/eforms-ubl-extension-basic-components/1",
    "efext": "http://data.europa.eu/p27/eforms-ubl-extensions/1",
    "ext": "urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2",
}

default_args = {
    "owner": "airflow",
    # The submission step can fail on a transient database problem, so every
    # task retries a few times before the run is marked failed.
    "retries": 3,
    "retry_delay": timedelta(seconds=5),
}

def get_most_recent_date():
    """
    This function will do a check to see if raw_emta_data is empty or not
    :return:
    """
    pg = PostgresHook(postgres_conn_id=CONN_ID)

    # Fast check: returns None if table is empty, or the most recent date
    result = pg.get_first("SELECT contract_issued_at "
                          "FROM public_raw.raw_procurements_data "
                          "ORDER BY contract_issued_at DESC LIMIT 1;")

    if result is None:
        print(f"Table raw_procurements_data is empty. Set year/month to default values: "
              f"year={DEFAULT_YEAR}, month={DEFAULT_MONTH}")
        return DEFAULT_YEAR, DEFAULT_MONTH

    contract_date = result[0]
    year = contract_date.year
    month = contract_date.month

    print(f"Table raw_procurements_data contains data. Most recent year/month values: "
          f"year={year}, month={month}")
    return year, month

def fetch_procurement_data():
    """
    This task will get data procurement data from RHR and will write it to db
    """
    # Find the most recent year and month from table content
    year, month = get_most_recent_date()

    # Get current year and month
    today = date.today()
    current_year = today.year
    current_month = today.month

    print(f"Current year/month values: year={current_year}, month={current_month}")

    # Loop from start (year, month) until current (year, month) inclusive
    while (year, month) <= (current_year, current_month):
        # Do your per-month processing here
        # Get xml data
        xml_text = fetch_xml(year, month)

        # Get root from the xml
        root = ET.fromstring(xml_text)

        # Extract procurement data from the xml
        procurements = extract_procurements(root)

        # Upsert data
        upsert_data(procurements)

        # Advance to the next month
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1

def fetch_xml(year, month):
    """Download the XML for the given year and month from the API."""
    url = BASE_URL.format(year=year, month=month)
    print(f"Fetching: {url}")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    print(f"Downloaded {len(response.content):,} bytes")
    return response.text

def extract_procurements(root):
    """
    Extract procurement data from each ContractAwardNotice.

    Returns:
        procurements: awards with all fields present (including EU funding info)

    Only keeps notices that have an awarded amount.
    """
    procurements = []

    # Go through each procurement contract (notice) and extract values that we need. If some value, required for our
    # purpose, is missing, then we should skip the procurement contract (not add it to our list of procurements) and
    # move to the next contract in the xml.
    for notice in root.findall("can:ContractAwardNotice", NAMESPACES):
        # --- Amount (skip adding the procurement contract if amount missing) ---
        amount = _get_awarded_amount(notice)
        if amount is None:
            continue

        # --- Contract ID (notice UUID) ---
        notice_id_el = notice.find("cbc:ID", NAMESPACES)
        contract_id = notice_id_el.text if notice_id_el is not None else None

        # If no contract ID, skip
        if contract_id is None:
            continue

        # --- Issue Date ---
        issue_date_el = notice.find("cbc:IssueDate", NAMESPACES)
        issue_date = issue_date_el.text if issue_date_el is not None else None

        # If no issue date, skip
        if issue_date is None:
            continue

        # --- Company ID ---
        org_map = _build_org_map(notice)
        party_id_el = notice.find("cac:ContractingParty/cac:Party/cac:PartyIdentification/cbc:ID", NAMESPACES,)

        # If no party ID, skip
        if party_id_el is None:
            continue

        party_id = party_id_el.text
        company_id = org_map.get(party_id).get("company_id")
        company_name = org_map.get(party_id).get("company_name")

        # If no company ID, skip
        if company_id is None:
            continue

        # --- EU funded ---
        funding_el = notice.find(".//cbc:FundingProgramCode[@listName='eu-funded']", NAMESPACES,)

        # If no funding program code, skip
        if funding_el is None:
            continue

        record = {
            "contract_id": contract_id,
            "contract_issued_at": issue_date,
            "company_registration_code": company_id,
            "company_name": company_name,
            "contract_award_amount": amount,
            "eu_funded": funding_el.text,
        }

        procurements.append(record)

    return procurements

def _get_awarded_amount(notice):
    """
    Get the awarded contract amount.

    Prefer NoticeResult/TotalAmount (sum of all lot PayableAmounts).
    Fall back to the first LotTender PayableAmount if TotalAmount is missing.
    """
    # Total awarded amount across all lots
    total_el = notice.find(".//efac:NoticeResult/cbc:TotalAmount", NAMESPACES)
    if total_el is not None and total_el.text:
        return float(total_el.text)

    # Fallback: first lot PayableAmount
    payable_el = notice.find(
        ".//efac:NoticeResult/efac:LotTender/cac:LegalMonetaryTotal/"
        "cbc:PayableAmount",
        NAMESPACES,
    )
    if payable_el is not None and payable_el.text:
        return float(payable_el.text)

    return None

def _build_org_map(notice):
    """Build a mapping from org reference (e.g. ORG-0001) to CompanyID."""
    org_map = {}
    for org in notice.findall(".//efac:Organizations/efac:Organization/efac:Company", NAMESPACES,):
        party_id_el = org.find("cac:PartyIdentification/cbc:ID", NAMESPACES)
        company_id_el = org.find("cac:PartyLegalEntity/cbc:CompanyID", NAMESPACES)
        company_name = org.find("cac:PartyName/cbc:Name", NAMESPACES)
        if party_id_el is not None and company_id_el is not None:
            org_map[party_id_el.text] = {
                "company_id": company_id_el.text,
                "company_name": company_name.text,
            }
    return org_map

def upsert_data(procurements_data):
    """
    Fetch data for which the data url has been specified and upsert to public_raw.raw_emta_data
    :param procurements_data:
    :return:
    """
    # Create Airflow's PostgresHook for talking to the database
    pg = PostgresHook(postgres_conn_id=CONN_ID)

    if procurements_data:
        # Grab column names from the first dictionary
        target_fields = list(procurements_data[0].keys())

        # Extract values into tuples, make sure the keys match the target_fields order
        rows = [tuple(d[field] for field in target_fields) for d in procurements_data]

        # Upsert the rows
        pg.insert_rows(
            table="public_raw.raw_procurements_data",
            rows=rows,
            target_fields=target_fields,
            replace=True,
            replace_index="contract_id",
            commit_every=1000,
        )

    print(f"Successfully upserted {len(procurements_data)} records into raw_procurements_data table.")

with DAG(
    dag_id="ingest_raw_procurements_contract_data",
    description="Query procurements data from RHR and insert into DB",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # Scheduled: once per day: at 00:00
    schedule="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ingest", "raw", "procurement"],
) as dag:

    t_fetch_procurement_data = PythonOperator(
        task_id="fetch_procurement_data",
        python_callable=fetch_procurement_data,
    )

    t_fetch_procurement_data
