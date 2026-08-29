import psycopg2
import polars as pl
import os

from scripts.common.config import postgres_config

def read_query_from_file(file_path):
    # Open and read the content of the SQL file
    with open(file_path, 'r') as file:
        query = file.read()
    return query

def load_db_to_parquet(**kwargs):
    conn = psycopg2.connect(**postgres_config())
    try:
        query = read_query_from_file("/opt/airflow/database/config_db/extract_company.sql")
        # PostgreSQL stores the current Type-1 company snapshot. Extract it in full;
        # filtering by CURRENT_DATE makes historical Airflow reruns silently empty.
        df = pl.read_database(query=query, connection=conn)
    finally:
        conn.close()
    print(f"[Company extract] retrieved {df.height} rows from the database")
    execution_date = kwargs.get("execution_date")
    date_str = execution_date.strftime("%Y%m%d")
    output_dir = "/opt/airflow/data/parquet/companies/"
    output_path = os.path.join(output_dir, f"crawl_companies-{date_str}.parquet")
    os.makedirs(output_dir, exist_ok=True)

    df.write_parquet(output_path)
    print(f"Saved data from database to parquet successfully at {output_path}")
