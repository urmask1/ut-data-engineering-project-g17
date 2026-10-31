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