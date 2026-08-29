from contextlib import contextmanager
from datetime import datetime

import duckdb
from pyspark.sql import DataFrame


@contextmanager
def transaction(conn: duckdb.DuckDBPyConnection):
    conn.execute("BEGIN TRANSACTION")
    try:
        yield
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def ensure_time(conn: duckdb.DuckDBPyConnection, value: datetime) -> int:
    calendar_date = value.date()
    conn.execute(
        """
        INSERT INTO dim_time (date, day_of_week, month, quarter, year)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (date) DO NOTHING
        """,
        [
            calendar_date,
            value.strftime("%A"),
            value.strftime("%B"),
            str((value.month - 1) // 3 + 1),
            value.year,
        ],
    )
    return int(
        conn.execute("SELECT id FROM dim_time WHERE date = ?", [calendar_date]).fetchone()[0]
    )


def register_spark_frame(
    conn: duckdb.DuckDBPyConnection, name: str, frame: DataFrame
) -> int:
    """Materialize a transformed Spark result at the local DuckDB boundary."""
    pandas_frame = frame.toPandas()
    conn.register(name, pandas_frame)
    return len(pandas_frame.index)
