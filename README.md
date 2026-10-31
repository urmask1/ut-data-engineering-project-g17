# Data engineering project of group17

This project explores the relationships of revenue 
and employee count across different companies and 
sectors in Estonia. As well as how the companies in 
different sectors take advantage of Estonia's state 
procurement environment.

## Components
The project involves following components:
1. Containerization of the whole project into Docker containers
2. PostgreSQL database for storing data
3. Apache Airflow for data pipleline orchestration
4. dbt for data transformations and checks
5. Superset for visualizing data in the final phase

## Data used for this project
Data used for this project is public data taken from:
1. Estonian business registry's open data for existing companies and their sector info
    1. Source - https://avaandmed.ariregister.rik.ee/et/avaandmete-allalaadimine
2. EMTA (Eesti Maksu- ja Tolliamet) for company revenue and employee count
    1. Source - https://emta.ee/eraklient/amet-uudised-ja-kontakt/uudised-pressiinfo-statistika/statistika-ja-avaandmed
3. Estonian state procurement registry for completed procurement contracts:
   1. Source - https://riigihanked.riik.ee/rhr-web/#/open-data

### Data from Estonian business registry's open data portal
The data for company and sector information is large (> 4Gb) JSON file so in order
to use it in our project we have parsed it separately with script `parse_ettevotja_rekvisiidid.py` 
and turned it into CSV files `raw_companies.csv` and `raw_sectors.csv` that can be
found in `dbt_project/seeds/` folder.

## How to run
You run the project by issuing following commands:
```
cp. env_template        # copy environment defaults (edit as needed)
docker compose build    # build needed images
docker compose up -d    # start all services in the background
```

Wait for everything to start up. After all the services show as up 
by docker Airflow web UI will take a bit more time on first start 
since it is waiting for airflow-init to run its commands in the 
background (migrates metadata database and creates admin user).

For stopping the services:
```
docker compose down
```

For stopping the services and removing the containers:
```
docker compose down -v
```

If you need to make completely clean start then before startup you 
need to remove following folders that were created as part of the 
startup process:
```
logs/                   # created by airflow-webserver
pgdata_airflow/         # created by airflow-db
pgdata_project/         # created by project-db (postgreSQL)
dbt/project/target/     # created by dbt
dbt/project/logs/       # created by dbt
```

## Access
Below the default credentials are used. Use whatever credentials you have set
in your `.env` file.

| Service    | URL / connection                    | Credentials                 |
|------------|-------------------------------------|-----------------------------|
| dbt        | `docker compose exec dbt dbt <cmd>` | —                           |
| Airflow UI | http://localhost:8080                | airflow / airflow           |
| pgAdmin    | http://localhost:5051                | admin@example.com / admin   |
| project-db | host `localhost`, port `5434`       | project_user / project_pass |
| airflow-db | host `localhost`, port `5435`       | airflow / airflow           |

## PostgreSQL
Run in docker container named `project-db`.
In file `scripts/create_tables.sql` commands for creating schema(s) and table(s) needed
for some of the Airflow ingestion scripts to work.

## Airflow
Orchestrates data pipelines (ingestion, transformation etc.).

### DAGs set up in Airflow
* `ingest_raw_company_sector.py` - takes the `raw_companies.csv` and `raw_sectors.csv` 
files from folder `dbt_project/seeds/` and turns them into raw tables (raw_companies 
and raw_sectors, respectively) in public_raw schema using dbt
  * No schedule configured for now, trigger manually
* `ingest_raw_emta_data.py` - queries EMTA open data site to query relevant CSV files 
and turns the data into raw_emta_data table in public_raw schema
  * Configured schedule to run once per day (at 00:00)
* `ingest_raw_procurement_data.py` - TO-DO (query data from procurement registry)

## dbt
To-be continued...