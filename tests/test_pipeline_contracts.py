import json
from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl

from scripts.elt_to_dwh.load.load_api_to_parquet import convert_ohlcs_to_parquet
from scripts.elt_to_dwh.create_dwh import create_dwh
from scripts.quality.validate_dwh import validate_dwh


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_postgres_generated_hash_avoids_stable_concat_ws():
    ddl = (REPO_ROOT / "database/config_db/ddl_db.sql").read_text(encoding="utf-8")
    assert "concat_ws" not in ddl.lower()


def test_empty_market_day_produces_typed_parquet(tmp_path):
    raw_dir = tmp_path / "raw"
    parquet_dir = tmp_path / "parquet"
    raw_dir.mkdir()
    source = raw_dir / "crawl_ohlcs-20250712.json"
    source.write_text(json.dumps([]), encoding="utf-8")

    convert_ohlcs_to_parquet(
        execution_date=datetime(2025, 7, 12),
        input_directory=str(raw_dir),
        output_directory=str(parquet_dir),
    )

    result = pl.read_parquet(parquet_dir / "crawl_ohlcs-20250712.parquet")
    assert result.is_empty()
    assert result.columns == [
        "ticker",
        "volume",
        "volume_weighted",
        "open",
        "close",
        "high",
        "low",
        "time_stamp",
        "num_of_trades",
        "is_otc",
    ]


def test_warehouse_contracts_and_quality_checks(tmp_path):
    db_path = tmp_path / "test.duckdb"
    ddl_path = REPO_ROOT / "database/config_dwh/ddl_dwh.sql"
    create_dwh(str(db_path), str(ddl_path))
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO dim_time (date, day_of_week, month, quarter, year)
            VALUES ('2025-07-10', 'Thursday', 'July', '3', 2025)
            """
        )
        conn.execute(
            """
            INSERT INTO dim_companies (
                name, ticker, is_delisted, exchange, region
            ) VALUES ('Example Inc', 'EXM', false, 'NASDAQ', 'United States')
            """
        )

    validate_dwh(str(db_path))

    with duckdb.connect(str(db_path)) as conn:
        try:
            conn.execute(
                """
                INSERT INTO dim_time (date, day_of_week, month, quarter, year)
                VALUES ('2025-07-10', 'Thursday', 'July', '3', 2025)
                """
            )
        except duckdb.ConstraintException:
            pass
        else:
            raise AssertionError("dim_time.date must be unique")
