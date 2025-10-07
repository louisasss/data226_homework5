# Data 226 Homework 5 – Airflow DAG with Snowflake
## Louisa Stumpf

This repo contains an Apache Airflow DAG that extracts stock data from the Alpha Vantage API, transforms it, and loads it into Snowflake.

## Setup
1. Place `homework5_dag.py` in your Airflow `dags/` folder.
2. In Airflow UI:
   - **Variable**: `alphavantage_api` = your Alpha Vantage API key
   - **Connection**: `snowflake_conn` = Snowflake account credentials

## Run
- Start Airflow (`docker compose up -d`).
- Log in at [http://localhost:8080](http://localhost:8080) (default: airflow/airflow).
- Enable and trigger the `homework5` DAG.

## Output
The DAG creates/refreshes a Snowflake table:
USER_DB_PIGEON.RAW.LLY
(symbol, date, open, close, high, low, volume)
