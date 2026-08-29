import os


def postgres_config(database: str = "datasource") -> dict:
    """Return PostgreSQL connection settings from the environment."""
    return {
        "host": os.getenv("POSTGRES_HOST", "database"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": database,
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }


def duckdb_path() -> str:
    return os.getenv("DUCKDB_PATH", "/opt/airflow/database/config_dwh/mydb.duckdb")
