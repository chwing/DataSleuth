from airflow.providers.standard.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
import duckdb
import requests
import os

DUCKDB_PATH = "/opt/airflow/warehouse/warehouse.duckdb"
DATA_DIR = "/opt/airflow/data"
os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)

def ingest_taxi_data(execution_date, **context):
    year = execution_date.year
    month = execution_date.month

    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    local_path = f"{DATA_DIR}/{file_name}"

    # Download parquet
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    # Load into DuckDB
    con = duckdb.connect(DUCKDB_PATH)

    con.execute("""
        CREATE TABLE IF NOT EXISTS yellow_taxi_trips AS
        SELECT * FROM read_parquet(?)
        LIMIT 0
    """, [local_path])

    con.execute("""
        INSERT INTO yellow_taxi_trips
        SELECT *, CURRENT_TIMESTAMP AS ingestion_time
        FROM read_parquet(?)
    """, [local_path])

    con.close()

with DAG(
    dag_id="ingest_nyc_taxi",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@monthly",
    catchup=True,
    tags=["ingestion", "nyc", "datasleuth"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_monthly_taxi_data",
        python_callable=ingest_taxi_data,
    )
