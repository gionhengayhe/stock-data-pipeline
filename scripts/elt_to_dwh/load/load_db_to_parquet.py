import psycopg2
import polars as pl

from scripts.common.config import DATA_ROOT, DATABASE_ROOT, postgres_config
from scripts.common.files import dated_file

def read_query_from_file(file_path):
    # Open and read the content of the SQL file
    with open(file_path, 'r') as file:
        query = file.read()
    return query

def load_db_to_parquet(**kwargs):
    conn = psycopg2.connect(**postgres_config())
    try:
        query = read_query_from_file(DATABASE_ROOT / "config_db" / "extract_company.sql")
        # PostgreSQL stores the current Type-1 company snapshot. Extract it in full;
        # filtering by CURRENT_DATE makes historical Airflow reruns silently empty.
        df = pl.read_database(query=query, connection=conn)
    finally:
        conn.close()
    print(f"[Company extract] retrieved {df.height} rows from the database")
    execution_date = kwargs.get("execution_date")
    output_path = dated_file(
        DATA_ROOT / "parquet" / "companies",
        "crawl_companies",
        execution_date,
        ".parquet",
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.write_parquet(output_path)
    print(f"Saved data from database to parquet successfully at {output_path}")
