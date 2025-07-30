import psycopg2
import polars as pl
import os
from jinja2 import Template
from datetime import timedelta

def read_query_from_file(file_path):
    # Open and read the content of the SQL file
    with open(file_path, 'r') as file:
        query = file.read()
    return query

def load_db_to_parquet(**kwargs):
    conn = psycopg2.connect(
        host="database",
        database="datasource",
        user="postgres",
        password="postgres",
    )
    query = read_query_from_file("/opt/airflow/database/config_db/extract_company.sql")
    df = pl.read_database(query=query, connection=conn)
    print(f"[Company extract] retrieved {df.height} rows from the database")
    execution_date = kwargs.get("execution_date")
    date_str = execution_date.strftime("%Y%m%d")
    output_dir = "/opt/airflow/data/parquet/companies/"
    output_path = os.path.join(output_dir, f"crawl_companies-{date_str}.parquet")
    os.makedirs(output_dir, exist_ok=True)

    df.write_parquet(output_path)
    print(f"Saved data from database to parquet successfully at {output_path}")