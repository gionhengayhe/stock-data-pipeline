import os
from pathlib import Path


DATA_ROOT = Path(os.getenv("DATA_ROOT", "/opt/airflow/data"))
DATABASE_ROOT = Path(os.getenv("DATABASE_ROOT", "/opt/airflow/database"))


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def postgres_config(database: str | None = None) -> dict:
    """Return PostgreSQL connection settings from the environment."""
    return {
        "host": os.getenv("POSTGRES_HOST", "database"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": database or os.getenv("POSTGRES_DB", "datasource"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }


def duckdb_path() -> str:
    return os.getenv(
        "DUCKDB_PATH", str(DATABASE_ROOT / "config_dwh" / "mydb.duckdb")
    )
