import json
import os
import shutil
from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl
import pytest
from pyspark.sql import SparkSession

from scripts.elt_to_dwh.load.load_api_to_parquet import convert_ohlcs_to_parquet
from scripts.elt_to_dwh.create_dwh import create_dwh
from scripts.elt_to_dwh.transform.process_ohlcs import transform_ohlcs
from scripts.quality.validate_dwh import validate_dwh
from scripts.common.storage import parquet_key


REPO_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[1]))


def test_postgres_ddl_does_not_depend_on_generated_hashes():
    ddl = (REPO_ROOT / "database/config_db/ddl_db.sql").read_text(encoding="utf-8")
    normalized = ddl.lower()
    assert "hash_row" not in normalized
    assert "generated always as" not in normalized


def test_s3_object_key_is_deterministic():
    assert (
        parquet_key("ohlcs", datetime(2025, 7, 12))
        == "ohlcs/crawl_ohlcs-20250712.parquet"
    )


def test_airflow_runs_every_warehouse_transform_with_spark():
    dag_path = REPO_ROOT / "airflow/dags/elt_to_dwh.py"
    if not dag_path.exists():
        dag_path = REPO_ROOT / "dags/elt_to_dwh.py"
    dag = dag_path.read_text(encoding="utf-8")
    assert "SparkSubmitOperator" in dag
    for job in ["process_companies", "process_ohlcs", "process_news"]:
        assert f'spark_job("{job}"' in dag


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


@pytest.fixture(scope="session")
def spark():
    if shutil.which("java") is None:
        pytest.skip("Java is required for local Spark tests")
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-contract-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_spark_ohlc_transform_uses_the_stable_company_key(spark):
    candles = spark.createDataFrame(
        [
            {
                "ticker": "AAA",
                "volume": 100,
                "volume_weighted": 10.5,
                "open": 10.0,
                "close": 11.0,
                "high": 11.5,
                "low": 9.5,
                "time_stamp": 1_752_115_600_000,
                "num_of_trades": 20,
                "is_otc": None,
            },
            {
                "ticker": "UNMAPPED",
                "volume": 1,
                "volume_weighted": 1.0,
                "open": 1.0,
                "close": 1.0,
                "high": 1.0,
                "low": 1.0,
                "time_stamp": 1_752_115_600_000,
                "num_of_trades": 1,
                "is_otc": True,
            },
        ]
    )
    companies = spark.createDataFrame([{"company_id": 42, "ticker": "AAA"}])

    row = transform_ohlcs(candles, companies, time_id=7).collect()[0]

    assert row.company_id == 42
    assert row.time_id == 7
    assert row.is_otc is False
    assert row.close == 11.0
