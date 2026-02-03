from airflow.providers.standard.operators.python import PythonOperator

from airflow import DAG
from datetime import datetime
import duckdb

DUCKDB_PATH = "/opt/airflow/warehouse/warehouse.duckdb"


def analyze_taxi_data(**context):
    """Analyze the ingested NYC taxi data"""

    print("=" * 70)
    print("📊 NYC TAXI DATA ANALYSIS")
    print("=" * 70)

    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    # Check if table exists
    tables = con.execute("SHOW TABLES").fetchall()
    print(f"\nAvailable tables: {tables}")

    if not tables or 'yellow_taxi_trips' not in str(tables):
        print("\n⚠️  No data found! Run 'ingest_nyc_taxi' DAG first.")
        con.close()
        return

    # Total records
    total = con.execute("SELECT COUNT(*) FROM yellow_taxi_trips").fetchone()[0]
    print(f"\n📈 Total trips in database: {total:,}")

    # Date range
    date_range = con.execute("""
        SELECT 
            MIN(tpep_pickup_datetime) as earliest,
            MAX(tpep_pickup_datetime) as latest
        FROM yellow_taxi_trips
    """).fetchone()
    print(f"📅 Date range: {date_range[0]} to {date_range[1]}")

    # Daily stats
    print("\n" + "=" * 70)
    print("DAILY TRIP STATISTICS (Last 7 days)")
    print("=" * 70)
    daily_stats = con.execute("""
        SELECT 
            DATE_TRUNC('day', tpep_pickup_datetime) as day,
            COUNT(*) as trips,
            ROUND(AVG(trip_distance), 2) as avg_distance,
            ROUND(AVG(total_amount), 2) as avg_fare,
            ROUND(SUM(total_amount), 2) as total_revenue
        FROM yellow_taxi_trips
        GROUP BY day
        ORDER BY day DESC
        LIMIT 7
    """).fetchall()

    for row in daily_stats:
        print(
            f"Day: {row[0]} | Trips: {row[1]:6,} | Avg Distance: {row[2]:6.2f} mi | Avg Fare: ${row[3]:6.2f} | Revenue: ${row[4]:12,.2f}")

    # Payment type breakdown
    print("\n" + "=" * 70)
    print("PAYMENT TYPE BREAKDOWN")
    print("=" * 70)
    payment_stats = con.execute("""
        SELECT 
            CASE payment_type
                WHEN 1 THEN 'Credit Card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No Charge'
                WHEN 4 THEN 'Dispute'
                ELSE 'Unknown'
            END as payment_method,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM yellow_taxi_trips
        GROUP BY payment_type
        ORDER BY count DESC
    """).fetchall()

    for row in payment_stats:
        print(f"{row[0]:15} | Count: {row[1]:8,} | {row[2]:5.2f}%")

    # Busiest hours
    print("\n" + "=" * 70)
    print("BUSIEST HOURS OF THE DAY")
    print("=" * 70)
    hourly = con.execute("""
        SELECT 
            EXTRACT(HOUR FROM tpep_pickup_datetime) as hour,
            COUNT(*) as trips
        FROM yellow_taxi_trips
        GROUP BY hour
        ORDER BY trips DESC
        LIMIT 5
    """).fetchall()

    for row in hourly:
        print(f"Hour {int(row[0]):02d}:00 | Trips: {row[1]:,}")

    # Top routes
    print("\n" + "=" * 70)
    print("TOP 10 ROUTES (by trip count)")
    print("=" * 70)
    routes = con.execute("""
        SELECT 
            PULocationID as pickup,
            DOLocationID as dropoff,
            COUNT(*) as trips,
            ROUND(AVG(trip_distance), 2) as avg_distance,
            ROUND(AVG(total_amount), 2) as avg_fare
        FROM yellow_taxi_trips
        WHERE PULocationID IS NOT NULL 
          AND DOLocationID IS NOT NULL
        GROUP BY PULocationID, DOLocationID
        ORDER BY trips DESC
        LIMIT 10
    """).fetchall()

    for i, row in enumerate(routes, 1):
        print(
            f"{i:2}. Zone {row[0]:3} → Zone {row[1]:3} | Trips: {row[2]:5,} | Avg Dist: {row[3]:5.2f} mi | Avg Fare: ${row[4]:6.2f}")

    print("\n" + "=" * 70)
    print("✓ Analysis complete!")
    print("=" * 70)

    con.close()


def export_sample_data(**context):
    """Export a sample of data for inspection"""

    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    # Export sample to CSV
    output_path = "/opt/airflow/data/taxi_sample.csv"

    con.execute(f"""
        COPY (
            SELECT * FROM yellow_taxi_trips
            LIMIT 1000
        ) TO '{output_path}' (HEADER, DELIMITER ',')
    """)

    print(f"✓ Exported 1000 sample rows to {output_path}")

    con.close()


with DAG(
        dag_id="analyze_nyc_taxi",
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=True,
        tags=["analysis", "nyc", "datasleuth"],
        description="Analyze NYC Yellow Taxi trip data in DuckDB",
) as dag:
    analyze = PythonOperator(
        task_id="analyze_taxi_data",
        python_callable=analyze_taxi_data,
    )

    export = PythonOperator(
        task_id="export_sample",
        python_callable=export_sample_data,
    )

    analyze >> export