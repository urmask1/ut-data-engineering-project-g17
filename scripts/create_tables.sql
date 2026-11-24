-- Create schema public_raw if it doesn't exist, all the raw data will go there
-- Need to create it in this script because Airflow ingestion of EMTA data will require this to exist
CREATE SCHEMA IF NOT EXISTS public_raw;

-- Table to store raw company revenue and employee count data
-- Need to create it in this script because Airflow ingestion of EMTA data will require this to exist
CREATE TABLE IF NOT EXISTS public_raw.raw_emta_data (
    id text PRIMARY KEY,
    year int NOT NULL,
    quarter int NOT NULL,
    company_registration_code int NOT NULL,
    company_name text NOT NULL,
    field_of_activity text NOT NULL,
    revenue float NOT NULL,
    employee_count integer NOT NULL
);

-- Table to store raw procurement contracts data
-- Need to create it in this script because Airflow ingestion of procurement data will require this to exist
-- Grain: one row per settled contract per winning company (joint tenders give several rows).
-- Registration codes are text because foreign winners have non-numeric codes (e.g. 'FI1234567-8').
CREATE TABLE IF NOT EXISTS public_raw.raw_procurements_data (
    record_id text PRIMARY KEY,                -- notice_id/contract_id/winner org ref, e.g. 'b92b.../CON-0001/ORG-0004'
    notice_id text NOT NULL,                   -- UUID of the contract award notice
    notice_type text,                          -- e.g. can-standard, can-social (can-modif is not loaded)
    notice_issued_at date NOT NULL,            -- notice publication date (source files are split by this)
    contract_folder_id text,                   -- procurement procedure ID, shared by all notices of one procurement
    contract_id text NOT NULL,                 -- contract ID inside the notice, e.g. CON-0001
    contract_title text,
    contract_issued_at date NOT NULL,          -- contract signing date (notice date if not given)
    buyer_registration_code text,              -- contracting authority (the public body)
    buyer_name text,
    company_registration_code text,            -- winner's registration code
    company_name text,                         -- winner's name
    company_country text,                      -- winner's country, ISO 3166-1 alpha-3 (e.g. EST)
    winners_in_contract int NOT NULL,          -- number of winning companies (>1 = joint tender)
    contract_award_amount numeric(15,2),       -- whole contract value (not split between winners); NULL = not published
    currency text,
    eu_funded text,                            -- 'eu-funds' / 'no-eu-funds'; NULL = not reported
    framework_agreement text                   -- 'none' = ordinary contract; 'fa-wo-rc' / 'fa-w-rc' / 'fa-mix' = framework
                                               -- agreement, where the amount is the agreement's ceiling, not actual spending
);
