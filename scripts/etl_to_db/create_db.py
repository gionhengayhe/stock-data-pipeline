import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from scripts.common.config import postgres_config


def create_db():
    """
    Create the database if not exists.
    """
    db_name = "datasource"
    conn = psycopg2.connect(**postgres_config(database="postgres"))
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
    exists = cur.fetchone()

    if not exists:
        cur.execute(f"CREATE DATABASE {db_name};")
        print(f"Created database {db_name}")
    else:
        print(f"Database {db_name} already exists")
    cur.close()
    conn.close()

def create_tables():
    """
    Create the tables in the database.
    """
    conn = psycopg2.connect(**postgres_config())
    with open("/opt/airflow/database/config_db/ddl_db.sql", 'r', encoding='utf-8') as file:
        sql = file.read()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("SQL script executed successfully.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def create_indexes():
    """
    Create the indexes in the database.
    """
    conn = psycopg2.connect(**postgres_config())
    sql = """
        CREATE INDEX IF NOT EXISTS idx_company_time_stamp ON companies(updated_time);
        CREATE INDEX IF NOT EXISTS idx_company_exchange_id ON companies(exchange_id);
        CREATE INDEX IF NOT EXISTS idx_exchange_region_id ON exchanges(region_id);
        CREATE INDEX IF NOT EXISTS idx_company_industry_id ON companies(industry_id);
        CREATE INDEX IF NOT EXISTS idx_company_sic_id ON companies(sic_id);
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("Required indexes are present.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

