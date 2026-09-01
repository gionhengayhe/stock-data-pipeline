import json
import os
import shutil
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl
import pytest
from pyspark.sql import SparkSession

from scripts.elt_to_dwh.stage import convert_ohlcs_to_parquet
from scripts.elt_to_dwh.create_dwh import create_dwh
from scripts.elt_to_dwh.transform.process_ohlcs import transform_ohlcs
from scripts.quality import validate_dwh as validate_module
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
    assert 'op_kwargs={"expected_date": "{{ ds }}"}' in dag


def test_dashboard_uses_the_canonical_portable_excel_source():
    relative_source_path = "../flask-api/exported_data/stock_data.xlsx"
    exporter = (REPO_ROOT / "flask-api/dump_api_to_excel.py").read_text(
        encoding="utf-8"
    )
    workbook_path = REPO_ROOT / "dashboard/dashboard.twb"
    tableau_workbook = workbook_path.read_text(encoding="utf-8")

    assert 'Path(__file__).resolve().parent' in exporter
    assert '/ "exported_data"' in exporter
    assert '/ "stock_data.xlsx"' in exporter
    assert tableau_workbook.count(relative_source_path) == 1
    assert "stock_dashboard.xlsx" not in tableau_workbook
    assert "D:/DataEngineer" not in tableau_workbook
    assert "federated.stockdashboard" not in tableau_workbook
    assert not (REPO_ROOT / "dashboard/my_dashboard.twb").exists()
    assert not (REPO_ROOT / "dashboard/_build_workbook.py").exists()
    assert not (REPO_ROOT / "flask-api/dashboard_model.py").exists()


def test_api_export_is_ordered_and_snapshot_consistent():
    api = (REPO_ROOT / "flask-api/app.py").read_text(encoding="utf-8")
    exporter = (REPO_ROOT / "flask-api/dump_api_to_excel.py").read_text(
        encoding="utf-8"
    )

    expected_ordering = {
        "dim_time": "date, id",
        "dim_news": "time_id, time_published, id",
        "dim_topics": "name, id",
        "dim_companies": "ticker, is_delisted, id",
        "fact_news_companies": "new_id, company_id, id",
        "fact_news_topics": "new_id, topic_id, id",
        "fact_candles": "time_id, company_id, id",
    }
    for table, order_by in expected_ordering.items():
        assert f'"{table}": "{order_by}"' in api
    assert "TABLE_ORDER_BY" in api
    assert "ORDER BY {order_by} LIMIT ? OFFSET ?" in api
    assert '@app.get("/snapshot")' in api
    assert 'request.args.get("snapshot")' in api
    assert 'response.headers["X-Data-Snapshot"]' in api

    assert "fetch_consistent_tables" in exporter
    assert 'params={' in exporter and '"snapshot": snapshot_token' in exporter
    assert "MAX_EXPORT_ATTEMPTS = 3" in exporter
    assert "os.replace(temporary_path, OUTPUT_PATH)" in exporter
    assert "from dashboard_model" not in exporter
    assert "build_dashboard_source" not in exporter
    assert "build_news_detail" not in exporter
    assert "dashboard_source" not in exporter
    assert "dashboard_news_detail" not in exporter
    assert "export_manifest" not in exporter
    assert not (REPO_ROOT / "flask-api/dashboard_model.py").exists()


def test_dashboard_uses_raw_dimension_and_fact_relationships():
    workbook_path = REPO_ROOT / "dashboard/dashboard.twb"
    workbook_xml = ET.parse(workbook_path).getroot()

    main_sources = [
        source
        for source in workbook_xml.findall("./datasources/datasource")
        if source.attrib.get("name") != "Parameters"
    ]
    assert len(main_sources) == 1
    table_names = {
        relation.attrib["name"]
        for relation in main_sources[0].iter("relation")
        if relation.attrib.get("type") == "table"
    }
    assert table_names == {
        "dim_time",
        "dim_time1",
        "dim_news",
        "dim_topics",
        "dim_companies",
        "fact_news_companies",
        "fact_news_topics",
        "fact_candles",
    }
    assert not {"dashboard_source", "dashboard_news_detail"} & table_names


def test_dashboard_has_role_aware_date_parameters():
    workbook_path = REPO_ROOT / "dashboard/dashboard.twb"
    workbook_xml = ET.parse(workbook_path).getroot()

    parameters = workbook_xml.find("./datasources/datasource[@name='Parameters']")
    assert parameters is not None
    parameter_columns = {
        column.attrib["caption"]: column
        for column in parameters.findall("column")
    }
    assert {"p_Start Date", "p_End Date"} <= set(parameter_columns)

    main_source = next(
        source
        for source in workbook_xml.findall("./datasources/datasource")
        if source.attrib.get("name") != "Parameters"
    )
    assert main_source is not None
    calculations = {
        column.attrib.get("caption"): column.find("calculation").attrib["formula"]
        for column in main_source.findall("column")
        if column.find("calculation") is not None
    }
    assert calculations["Filter - Market Date Range"] == (
        f"[date] >= [Parameters].{parameter_columns['p_Start Date'].attrib['name']} "
        f"AND [date] <= [Parameters].{parameter_columns['p_End Date'].attrib['name']}"
    )
    assert calculations["Filter - News Date Range"] == (
        f"[date (dim!time1)] >= "
        f"[Parameters].{parameter_columns['p_Start Date'].attrib['name']} AND "
        f"[date (dim!time1)] <= "
        f"[Parameters].{parameter_columns['p_End Date'].attrib['name']}"
    )


def test_empty_market_day_produces_typed_parquet(tmp_path):
    raw_dir = tmp_path / "raw" / "ohlcs"
    parquet_dir = tmp_path / "parquet"
    raw_dir.mkdir(parents=True)
    source = raw_dir / "crawl_ohlcs-20250712.json"
    source.write_text(json.dumps([]), encoding="utf-8")

    convert_ohlcs_to_parquet(
        execution_date=datetime(2025, 7, 12),
        input_root=str(tmp_path),
        output_root=str(tmp_path),
    )

    result = pl.read_parquet(
        parquet_dir / "ohlcs" / "crawl_ohlcs-20250712.parquet"
    )
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


def _create_valid_warehouse(tmp_path):
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
                name, ticker, is_delisted, exchange, region, industry, sector
            ) VALUES (
                'Example Inc', 'EXM', false, 'NASDAQ', 'United States',
                'Software', 'Technology'
            )
            """
        )
        conn.execute("INSERT INTO dim_topics (name) VALUES ('Technology')")
        conn.execute(
            """
            INSERT INTO dim_news (
                title, url, time_published, authors, summary, source,
                overall_sentiment_score, overall_sentiment_label, time_id
            )
            SELECT
                'Example news', 'https://example.com/news',
                TIMESTAMP '2025-07-10 12:00:00', ['Analyst'], 'Summary',
                'Example', 0.2, 'Somewhat-Bullish', id
            FROM dim_time WHERE date = DATE '2025-07-10'
            """
        )
        conn.execute(
            """
            INSERT INTO fact_candles (
                company_id, volume, volume_weighted, open, close, high, low,
                time_stamp, num_of_trades, is_otc, time_id
            )
            SELECT
                c.id, 1000, 10.5, 10.0, 11.0, 11.5, 9.5,
                TIMESTAMP '2025-07-10 16:00:00', 100, false, t.id
            FROM dim_companies c
            CROSS JOIN dim_time t
            WHERE c.ticker = 'EXM' AND t.date = DATE '2025-07-10'
            """
        )
        conn.execute(
            """
            INSERT INTO fact_news_companies (
                company_id, new_id, relevance_score,
                ticker_sentiment_score, ticker_sentiment_label
            )
            SELECT c.id, n.id, 0.9, 0.2, 'Somewhat-Bullish'
            FROM dim_companies c CROSS JOIN dim_news n
            WHERE c.ticker = 'EXM'
            """
        )
        conn.execute(
            """
            INSERT INTO fact_news_topics (new_id, topic_id, relevance_score)
            SELECT n.id, t.id, 0.8
            FROM dim_news n CROSS JOIN dim_topics t
            WHERE t.name = 'Technology'
            """
        )
    return db_path


def test_warehouse_contracts_and_quality_checks(tmp_path):
    db_path = _create_valid_warehouse(tmp_path)

    validate_dwh(str(db_path), expected_date="2025-07-10")

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


def test_quality_gate_rejects_schema_drift(tmp_path, monkeypatch):
    db_path = _create_valid_warehouse(tmp_path)
    monkeypatch.setitem(
        validate_module.EXPECTED_SCHEMA["fact_candles"],
        "volume",
        "INTEGER",
    )

    with pytest.raises(RuntimeError, match="schema fact_candles"):
        validate_dwh(str(db_path), expected_date="2025-07-10")


def test_quality_gate_requires_the_logical_date(tmp_path):
    db_path = _create_valid_warehouse(tmp_path)

    with pytest.raises(
        RuntimeError, match="freshness expected date 2025-07-11"
    ):
        validate_dwh(str(db_path), expected_date="2025-07-11")


def test_quality_gate_rejects_low_news_topic_coverage(tmp_path):
    db_path = _create_valid_warehouse(tmp_path)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("DELETE FROM fact_news_topics")

    with pytest.raises(RuntimeError, match="news-topic coverage"):
        validate_dwh(str(db_path), expected_date="2025-07-10")


def test_quality_gate_rejects_partial_candle_partition(tmp_path):
    db_path = _create_valid_warehouse(tmp_path)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO dim_companies (
                name, ticker, is_delisted, exchange, region, industry, sector
            ) VALUES
                ('Example 2', 'EX2', false, 'NASDAQ', 'United States',
                 'Software', 'Technology'),
                ('Example 3', 'EX3', false, 'NASDAQ', 'United States',
                 'Software', 'Technology'),
                ('Example 4', 'EX4', false, 'NASDAQ', 'United States',
                 'Software', 'Technology')
            """
        )
        conn.execute(
            """
            INSERT INTO dim_time (date, day_of_week, month, quarter, year)
            VALUES
                ('2025-07-07', 'Monday', 'July', '3', 2025),
                ('2025-07-08', 'Tuesday', 'July', '3', 2025),
                ('2025-07-09', 'Wednesday', 'July', '3', 2025)
            """
        )
        conn.execute(
            """
            INSERT INTO fact_candles (
                company_id, volume, volume_weighted, open, close, high, low,
                time_stamp, num_of_trades, is_otc, time_id
            )
            SELECT
                c.id, 1000, 10.5, 10.0, 11.0, 11.5, 9.5,
                CAST(t.date AS TIMESTAMP) + INTERVAL 16 HOUR,
                100, false, t.id
            FROM dim_companies c
            CROSS JOIN dim_time t
            WHERE t.date BETWEEN DATE '2025-07-07' AND DATE '2025-07-09'
            """
        )

    with pytest.raises(RuntimeError, match="candle volume ratio"):
        validate_dwh(str(db_path), expected_date="2025-07-10")


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
