"""
Airflow DAG that orchestrates fetching and upserting procurement contracts data from
Estonia's national procurement registry.

The data will be written to table public_raw.raw_procurements_data and will include procurement
contracts starting from 2023-06.

Grain of the raw table: one row per settled contract per winning company. A contract won jointly
by several companies has one row per company (winners_in_contract tells how many there are), and
a notice with several contracts (e.g. one per lot) has one row per contract.

The source files are monthly by notice publication date. If the table is not empty (meaning some raw
data has already been ingested), then the script will first find the year/month of the most recently
published notice. That will be the start year/month for fetching data. Rows are upserted by record_id,
so re-running for the same months does not create duplicates.

Pipeline shape:
    fetch_procurement_data

Schedule: Set to run once per day
If need to trigger manually run it from Airflow UI or from outside:
    docker compose exec airflow-scheduler airflow dags trigger ingest_raw_procurements_contract_data
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import requests
import pendulum
import xml.etree.ElementTree as ET
from datetime import timedelta, date

CONN_ID = "project_db"

DEFAULT_YEAR = 2023
DEFAULT_MONTH = 6

BASE_URL = (
    "https://riigihanked.riik.ee:443/rhr/api/public/v1/opendata/notice_award"
    "/{year}/month/{month}/xml"
)

# Contract modification notices repeat contracts that were already awarded in an earlier notice,
# so they are not loaded as new contracts.
SKIPPED_NOTICE_TYPES = {"can-modif"}

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
    Find the year/month of the most recently published notice in raw_procurements_data.
    Falls back to DEFAULT_YEAR/DEFAULT_MONTH when the table is empty.

    The notice publication date is used (not the contract signing date), because the source
    files are split by publication month and a contract can be signed months before its notice.
    """
    pg = PostgresHook(postgres_conn_id=CONN_ID)

    # Returns (None,) if table is empty, or the most recent notice date
    result = pg.get_first("SELECT MAX(notice_issued_at) FROM public_raw.raw_procurements_data;")

    if result is None or result[0] is None:
        print(f"Table raw_procurements_data is empty. Set year/month to default values: "
              f"year={DEFAULT_YEAR}, month={DEFAULT_MONTH}")
        return DEFAULT_YEAR, DEFAULT_MONTH

    notice_date = result[0]
    year = notice_date.year
    month = notice_date.month

    print(f"Table raw_procurements_data contains data. Most recent year/month values: "
          f"year={year}, month={month}")
    return year, month


def fetch_procurement_data():
    """
    This task will get procurement data from RHR and will write it to db
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
    Extract one record per settled contract per winning company from each ContractAwardNotice.

    How the winner is found in an eForms notice:
        NoticeResult/SettledContract  -> LotTender ID(s) of the contract
        NoticeResult/LotTender        -> TenderingParty ID, PayableAmount, TenderLot ID
        NoticeResult/TenderingParty   -> Tenderer organization ID(s)  (several = joint tender)
        Organizations/Organization    -> CompanyID (registration code), name, country

    NB! cac:ContractingParty is the buyer (the public body), not the winner.

    Contracts without a published value are kept with contract_award_amount = NULL, and lots
    without a funding flag get eu_funded = NULL, so that the dbt layer can decide how to treat them.
    """
    procurements = []
    skipped = {"modification_notice": 0, "no_contract_id": 0, "no_winner": 0}

    for notice in root.findall("can:ContractAwardNotice", NAMESPACES):
        notice_type = _text(notice, "cbc:NoticeTypeCode")
        if notice_type in SKIPPED_NOTICE_TYPES:
            skipped["modification_notice"] += 1
            continue

        notice_id = _text(notice, "cbc:ID")
        notice_issued_at = _date(_text(notice, "cbc:IssueDate"))
        if notice_id is None or notice_issued_at is None:
            skipped["no_contract_id"] += 1
            continue

        org_map = _build_org_map(notice)
        buyer_ref = _text(notice, "cac:ContractingParty/cac:Party/cac:PartyIdentification/cbc:ID")
        buyer = org_map.get(buyer_ref, {})

        result = notice.find(".//efac:NoticeResult", NAMESPACES)
        if result is None:
            skipped["no_winner"] += 1
            continue

        lot_tenders = _build_lot_tender_map(result)
        tendering_parties = _build_tendering_party_map(result)
        lot_funding = _build_lot_funding_map(notice)
        lot_framework = _build_lot_framework_map(notice)

        for contract in result.findall("efac:SettledContract", NAMESPACES):
            contract_id = _text(contract, "cbc:ID")
            if contract_id is None:
                skipped["no_contract_id"] += 1
                continue

            # The signing date of the contract; fall back to notice date if not given
            contract_issued_at = _date(_text(contract, "cbc:IssueDate")) or notice_issued_at

            tenders = [
                lot_tenders[t.text]
                for t in contract.findall("efac:LotTender/cbc:ID", NAMESPACES)
                if t.text in lot_tenders
            ]

            # Winners of the contract (unique organization IDs over all its tenders)
            winner_refs = []
            for tender in tenders:
                for org_ref in tendering_parties.get(tender["tendering_party"], []):
                    if org_ref not in winner_refs:
                        winner_refs.append(org_ref)

            if not winner_refs:
                skipped["no_winner"] += 1
                continue

            # Contract value = sum over its lot tenders; NULL if none of them is published
            amounts = [t["amount"] for t in tenders if t["amount"] is not None]
            amount = sum(amounts) if amounts else None
            currency = next((t["currency"] for t in tenders if t["currency"]), None)

            # EU funding: 'eu-funds' if any lot of the contract is EU funded,
            # 'no-eu-funds' if all flagged lots are not, NULL if no lot has the flag
            funding_codes = {lot_funding.get(t["lot"]) for t in tenders} - {None}
            if "eu-funds" in funding_codes:
                eu_funded = "eu-funds"
            elif funding_codes:
                eu_funded = "no-eu-funds"
            else:
                eu_funded = None

            # Framework agreement type of the contract's lots. For framework agreements the
            # published value is the maximum (ceiling) of the agreement, not actual spending,
            # so the dbt layer needs this flag to exclude or separate them.
            # 'none' if no lot is a framework agreement, NULL if no lot has the code,
            # otherwise the framework type (fa-wo-rc / fa-w-rc / fa-mix; first one found).
            framework_codes = {lot_framework.get(t["lot"]) for t in tenders} - {None}
            framework_types = sorted(framework_codes - {"none"})
            if framework_types:
                framework_agreement = framework_types[0]
            elif framework_codes:
                framework_agreement = "none"
            else:
                framework_agreement = None

            for org_ref in winner_refs:
                winner = org_map.get(org_ref, {})
                procurements.append({
                    "record_id": f"{notice_id}/{contract_id}/{org_ref}",
                    "notice_id": notice_id,
                    "notice_type": notice_type,
                    "notice_issued_at": notice_issued_at,
                    "contract_folder_id": _text(notice, "cbc:ContractFolderID"),
                    "contract_id": contract_id,
                    "contract_title": _text(contract, "cbc:Title"),
                    "contract_issued_at": contract_issued_at,
                    "buyer_registration_code": buyer.get("company_id"),
                    "buyer_name": buyer.get("company_name"),
                    "company_registration_code": winner.get("company_id"),
                    "company_name": winner.get("company_name"),
                    "company_country": winner.get("country"),
                    "winners_in_contract": len(winner_refs),
                    "contract_award_amount": amount,
                    "currency": currency,
                    "eu_funded": eu_funded,
                    "framework_agreement": framework_agreement,
                })

    print(f"Extracted {len(procurements)} contract-winner records. Skipped: {skipped}")
    return procurements


def _text(element, path):
    """Return stripped text of the first element matching path, or None."""
    found = element.find(path, NAMESPACES)
    if found is None or found.text is None:
        return None
    return found.text.strip() or None


def _date(value):
    """Turn an XML date like '2026-06-12+03:00' into '2026-06-12' (None stays None)."""
    return value[:10] if value else None


def _build_org_map(notice):
    """Build a mapping from org reference (e.g. ORG-0001) to registration code, name and country."""
    org_map = {}
    for org in notice.findall(".//efac:Organizations/efac:Organization/efac:Company", NAMESPACES):
        party_id = _text(org, "cac:PartyIdentification/cbc:ID")
        if party_id is None:
            continue
        org_map[party_id] = {
            "company_id": _text(org, "cac:PartyLegalEntity/cbc:CompanyID"),
            "company_name": _text(org, "cac:PartyName/cbc:Name"),
            "country": _text(org, "cac:PostalAddress/cac:Country/cbc:IdentificationCode"),
        }
    return org_map


def _build_lot_tender_map(result):
    """Map tender ID (e.g. TEN-0001) to its tendering party, lot and payable amount."""
    lot_tenders = {}
    for tender in result.findall("efac:LotTender", NAMESPACES):
        tender_id = _text(tender, "cbc:ID")
        if tender_id is None:
            continue
        amount_el = tender.find("cac:LegalMonetaryTotal/cbc:PayableAmount", NAMESPACES)
        amount = None
        if amount_el is not None and amount_el.text and amount_el.text.strip():
            amount = float(amount_el.text)
        lot_tenders[tender_id] = {
            "tendering_party": _text(tender, "efac:TenderingParty/cbc:ID"),
            "lot": _text(tender, "efac:TenderLot/cbc:ID"),
            "amount": amount,
            "currency": amount_el.get("currencyID") if amount_el is not None else None,
        }
    return lot_tenders


def _build_tendering_party_map(result):
    """Map tendering party ID (e.g. TPA-0001) to the list of organization IDs in it."""
    parties = {}
    for party in result.findall("efac:TenderingParty", NAMESPACES):
        party_id = _text(party, "cbc:ID")
        if party_id is None:
            continue
        parties[party_id] = [
            t.text.strip()
            for t in party.findall("efac:Tenderer/cbc:ID", NAMESPACES)
            if t.text
        ]
    return parties


def _build_lot_funding_map(notice):
    """Map lot ID (e.g. LOT-0000) to its EU funding code ('eu-funds' / 'no-eu-funds')."""
    funding = {}
    for lot in notice.findall("cac:ProcurementProjectLot", NAMESPACES):
        lot_id = _text(lot, "cbc:ID")
        code = _text(lot, "cac:TenderingTerms/cbc:FundingProgramCode[@listName='eu-funded']")
        if lot_id is not None:
            funding[lot_id] = code
    return funding


def _build_lot_framework_map(notice):
    """Map lot ID to its framework agreement code ('none' / 'fa-wo-rc' / 'fa-w-rc' / 'fa-mix')."""
    framework = {}
    for lot in notice.findall("cac:ProcurementProjectLot", NAMESPACES):
        lot_id = _text(lot, "cbc:ID")
        code = _text(
            lot,
            "cac:TenderingProcess/cac:ContractingSystem/"
            "cbc:ContractingSystemTypeCode[@listName='framework-agreement']",
        )
        if lot_id is not None:
            framework[lot_id] = code
    return framework


def upsert_data(procurements_data):
    """
    Upsert procurement records into public_raw.raw_procurements_data (key: record_id)
    :param procurements_data: list of dicts returned by extract_procurements
    :return:
    """
    if not procurements_data:
        print("No records to upsert.")
        return

    # Create Airflow's PostgresHook for talking to the database
    pg = PostgresHook(postgres_conn_id=CONN_ID)

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
        replace_index="record_id",
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
