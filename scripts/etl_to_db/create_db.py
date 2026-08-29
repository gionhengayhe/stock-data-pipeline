import os

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from scripts.common.config import DATABASE_ROOT, postgres_config


def _create_database_if_missing() -> None:
    database = os.getenv("POSTGRES_DB", "datasource")
    conn = psycopg2.connect(**postgres_config(database="postgres"))
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
            if cursor.fetchone() is None:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
                print(f"Created database {database}")
    finally:
        conn.close()


def initialize_metadata_db(**_) -> None:
    """Create the metadata database, tables, constraints, and indexes."""
    _create_database_if_missing()
    ddl = (DATABASE_ROOT / "config_db" / "ddl_db.sql").read_text(encoding="utf-8")
    with psycopg2.connect(**postgres_config()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(ddl)
    print("Metadata database is ready.")
