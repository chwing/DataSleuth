from airflow.providers.standard.operators.python import PythonOperator

from airflow import DAG
from datetime import datetime, timedelta
import duckdb
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def test_duckdb():
    """Test DuckDB connection and basic operations"""
    db_path = '/warehouse/test.duckdb'

    # Create warehouse directory if it doesn't exist
    os.makedirs('/warehouse', exist_ok=True)

    # Connect to DuckDB
    con = duckdb.connect(db_path)

    # Create a test table
    con.execute("""
        CREATE TABLE IF NOT EXISTS test_table (
            id INTEGER,
            name VARCHAR,
            value DOUBLE
        )
    """)

    # Insert test data
    con.execute("""
        INSERT INTO test_table VALUES 
        (1, 'test1', 100.5),
        (2, 'test2', 200.7),
        (3, 'test3', 300.9)
    """)

    # Query the data
    result = con.execute("SELECT * FROM test_table").fetchall()
    print(f"Query result: {result}")

    # Close connection
    con.close()

    print("DuckDB test completed successfully!")
    return "Success"


with DAG(
        'test_duckdb_dag',
        default_args=default_args,
        description='Test DuckDB integration',
        schedule_interval=None,
        catchup=False,
        tags=['test', 'duckdb'],
) as dag:
    test_task = PythonOperator(
        task_id='test_duckdb_connection',
        python_callable=test_duckdb,
    )