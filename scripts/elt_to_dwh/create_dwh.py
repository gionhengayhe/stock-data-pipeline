import duckdb

from scripts.common.config import duckdb_path


def create_dwh(db_path: str | None = None, ddl_path: str | None = None, **_):
    """
    Create the DWH database and execute the DDL script.
    """
    # Connect to DuckDB and execute the DDL script
    target_db = db_path or duckdb_path()
    target_ddl = ddl_path or '/opt/airflow/database/config_dwh/ddl_dwh.sql'
    with duckdb.connect(target_db) as conn:
        with open(target_ddl, 'r', encoding='utf-8') as f:
            ddl_script = f.read()
        conn.execute(ddl_script)
        # CREATE TABLE IF NOT EXISTS does not retrofit constraints into an old
        # local DuckDB file, so these indexes also act as lightweight migrations.
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_time_date ON dim_time(date)")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_company_snapshot "
            "ON dim_companies(ticker, is_delisted)"
        )
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_news_url ON dim_news(url)")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_fact_candle_grain "
            "ON fact_candles(company_id, time_id)"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_fact_news_company "
            "ON fact_news_companies(new_id, company_id)"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_fact_news_topic "
            "ON fact_news_topics(new_id, topic_id)"
        )
    print("Executed DDL script successfully!")
