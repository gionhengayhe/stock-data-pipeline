import psycopg2
import json
import os
import glob
import hashlib
import re
from datetime import datetime

def get_file_by_date(directory: str, date_str: str) -> str:
    """
    Get the file path by matching with the execution date.

    :param directory: Directory to search for files.
    :param date_str: Date string in the format YYYYMMDD.
    :return: Full path to the matching file.
    """
    pattern = re.compile(rf".*-{date_str}\.json$")
    for f in os.listdir(directory):
        if pattern.match(f):
            return os.path.join(directory, f)
    raise FileNotFoundError(f"No file found for date {date_str} in {directory}")

def read_file(directory, date_str: str):
    """
    Read the latest JSON file in a directory.

    :param directory: Directory to search for files.
    :return: Data from the JSON file or an empty list if no file is found.
    """
    latest_file = get_file_by_date(directory, date_str)
    if latest_file:
        with open(latest_file, 'r') as file:
            data = json.load(file)
        return data
    else:
        print(f"No JSON files found in {directory}.")
        return []

def compute_hash_row(row: dict) -> str:
    """
    Compute a hash for a row of data to detect changes.
    :param row: A dictionary representing a row of data.
    :return: A string representing the MD5 hash of the row.
    """
    s = json.dumps(row, sort_keys=True, default=str)
    return hashlib.md5(s.encode()).hexdigest()

def insert_or_update_data(data, table_name, columns, conflict_columns, has_hash_row: bool):
    """
    Insert or update data in a PostgreSQL table.
    :param data: List of dictionaries representing the data to be inserted or updated.
    :param table_name: Name of the PostgreSQL table.
    :param columns: List of column names in the table.
    :param conflict_columns: List of column names to check for conflicts (usually unique identifiers).
    :param has_hash_row: bool, whether the table has a hash_row column to detect changes.
    """
    conn = psycopg2.connect(
        host="database",
        database="datasource",
        user="postgres",
        password="postgres",
    )
    cur = conn.cursor()

    count = 0


    for row in data:
        values = [row.get(col) for col in columns]

        col_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        conflict_str = ", ".join(conflict_columns)

        # Check if table has a hash_row column
        if has_hash_row:
            # Update only if hash_row differs (data changed), and set updated_time
            update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])
            query = f"""
                INSERT INTO {table_name} ({col_str})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_str})
                DO UPDATE SET {update_set}, updated_time = %s
                WHERE {table_name}.hash_row IS DISTINCT FROM EXCLUDED.hash_row;
            """
        else:
            # Insert new rows only; ignore if conflict (no update needed)
            query = f"""
                INSERT INTO {table_name} ({col_str})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_str}) DO NOTHING;
            """

        try:
            # print("Query:", query)
            cur.execute(query, values if not has_hash_row else values + [datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
            if cur.rowcount > 0:
                count += 1
        except Exception as e:
            print(f"Error with row {row}: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    conn.close()

    print(f"{count} row(s) affected in '{table_name}'")

def get_foreign_key_map(table_name, key_columns: list, id_column="id"):
    """
    Get a mapping of foreign key values to their corresponding IDs from a PostgreSQL table.
    :param table_name: Name of the PostgreSQL table.
    :param key_columns: List of column names that form the foreign key.
    :param id_column: Name of the column that contains the ID (default is "id").
    :return: A dictionary mapping foreign key values to their corresponding IDs.
    """
    conn = psycopg2.connect(
        host="database",
        database="datasource",
        user="postgres",
        password="postgres",
    )
    cur = conn.cursor()
    cols_str = ", ".join([id_column] + key_columns)
    cur.execute(f"SELECT {cols_str} FROM {table_name}")

    if len(key_columns) == 1:
        result = {row[1]: row[0] for row in cur.fetchall()}
    else:
        result = {tuple(row[1:]): row[0] for row in cur.fetchall()}

    cur.close()
    conn.close()
    return result


def load_to_db(**kwargs):
    execution_date = kwargs["execution_date"]
    #Load regions
    regions = read_file("/opt/airflow/data/processed/regions", execution_date.strftime("%Y%m%d"))
    insert_or_update_data(
        data=regions,
        table_name="regions",
        columns=["region", "local_open", "local_close"],
        conflict_columns=["region"],
        has_hash_row=True,
    )
    #Load exchanges
    exchanges = read_file("/opt/airflow/data/processed/exchanges", execution_date.strftime("%Y%m%d"))
    region_map = get_foreign_key_map("regions", ["region"])
    for row in exchanges:
        region = row.get("region")
        region_id = region_map.get(region)
        if region_id:
            row["region_id"] = region_id
        else:
            print(f"Region '{region}' not found in regions table. Skipping row: {row}")
            continue
    insert_or_update_data(
        data=exchanges,
        table_name="exchanges",
        columns=["name", "region_id"],
        conflict_columns=["name"],
        has_hash_row=True,
    )
    # #Load industries
    industries = read_file("/opt/airflow/data/processed/industries", execution_date.strftime("%Y%m%d"))
    insert_or_update_data(
        data=industries,
        table_name="industries",
        columns=["industry","sector"],
        conflict_columns=["industry","sector"],
        has_hash_row=False,
    )
    # #Load sic_industries
    sic_industries = read_file("/opt/airflow/data/processed/sic_industries", execution_date.strftime("%Y%m%d"))
    insert_or_update_data(
        data=sic_industries,
        table_name="sic_industries",
        columns=["sic_industry", "sic_sector"],
        conflict_columns=["sic_industry", "sic_sector"],
        has_hash_row=False,
    )
    # #Load companies
    companies = read_file("/opt/airflow/data/processed/companies", execution_date.strftime("%Y%m%d"))
    industry_map = get_foreign_key_map("industries", ["industry", "sector"])
    exchange_map = get_foreign_key_map("exchanges", ["name"])
    sic_map = get_foreign_key_map("sic_industries", ["sic_industry", "sic_sector"], "id")
    for row in companies:
        exchange = row.get("exchange")
        industry = row.get("industry")
        sector = row.get("sector")
        sic_industry = row.get("sic_industry")
        sic_sector = row.get("sic_sector")

        exchange_id = exchange_map.get(exchange)
        industry_id = industry_map.get((industry, sector))
        sic_id = sic_map.get((sic_industry, sic_sector))
        if not exchange_id:
            print(f"Exchange '{exchange}' not found in exchanges table. Skipping row: {row}")
            continue
        if not industry_id:
            print(f"Industry '{industry}' and sector '{sector}' not found in industries table. Skipping row: {row}")
            continue
        if not sic_id:
            print(f"SIC Industry '{sic_industry}' and SIC Sector '{sic_sector}' not found in sic_industries table. Skipping row: {row}")
            continue
        row["exchange_id"] = exchange_id
        row["industry_id"] = industry_id
        row["sic_id"] = sic_id
    insert_or_update_data(
        data=companies,
        table_name="companies",
        columns=["exchange_id", "industry_id","sic_id", "name", "ticker", "is_delisted", "category", "currency", "location"],
        conflict_columns=["is_delisted", "ticker"],
        has_hash_row=True,
    )

if __name__ == "__main__":
    load_to_db()
