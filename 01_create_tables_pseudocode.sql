-- Project 1, Group 17: star schema in pseudocode (simplified version of 01_create_tables.sql)

-- Dimensions
TABLE DimDate (
    DateID   INT PK,          -- YYYYMMDD
    date     DATE,
    year     INT,
    quarter  INT
)                             -- static

TABLE DimSector (
    SectorID     INT PK,
    sector_name  TEXT         -- EMTAK sector from EMTA data
)                             -- static

TABLE DimCompany (
    CompanyID          INT PK,
    company_name       TEXT,  -- SCD Type 1
    registration_code  TEXT,  -- business key, joins EMTA, RHR and Business Register
    registration_date  DATE,  -- from Business Register
    sector_id          INT FK -> DimSector,   -- SCD Type 2
    valid_from         DATE,  -- SCD2: version valid from
    valid_to           DATE,  -- SCD2: version valid until
    is_current         BOOLEAN
)

TABLE DimProcurement (
    ProcurementID            INT PK,
    procurement_contract_id  TEXT,   -- contract ID in RHR
    eu_funded                BOOLEAN
)                                    -- static

-- Facts
TABLE FactCompany (                  -- grain: one row per company per quarter
    company_id      INT FK -> DimCompany,
    date_id         INT FK -> DimDate,   -- last day of quarter
    revenue         DECIMAL,
    employee_count  INT
)

TABLE FactProcurement (              -- grain: one row per company per contract
    company_id               INT FK -> DimCompany,
    date_id                  INT FK -> DimDate,   -- contract signing date
    procurement_id           INT FK -> DimProcurement,
    awarded_contract_amount  DECIMAL
)
