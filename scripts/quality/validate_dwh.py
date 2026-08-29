import os

import duckdb


DWH_PATH = os.getenv(
    "DUCKDB_PATH", "/opt/airflow/database/config_dwh/mydb.duckdb"
)


QUALITY_CHECKS = {
    "duplicate dim_time date": """
        SELECT COUNT(*) FROM (
            SELECT date FROM dim_time GROUP BY date HAVING COUNT(*) > 1
        )
    """,
    "duplicate company snapshot": """
        SELECT COUNT(*) FROM (
            SELECT ticker, is_delisted FROM dim_companies
            GROUP BY ticker, is_delisted HAVING COUNT(*) > 1
        )
    """,
    "duplicate news url": """
        SELECT COUNT(*) FROM (
            SELECT url FROM dim_news GROUP BY url HAVING COUNT(*) > 1
        )
    """,
    "duplicate candle grain": """
        SELECT COUNT(*) FROM (
            SELECT company_id, time_id FROM fact_candles
            GROUP BY company_id, time_id HAVING COUNT(*) > 1
        )
    """,
    "invalid OHLC values": """
        SELECT COUNT(*) FROM fact_candles
        WHERE high < GREATEST(open, close, low)
           OR low > LEAST(open, close, high)
           OR volume < 0 OR num_of_trades < 0
    """,
    "orphan candle foreign key": """
        SELECT COUNT(*)
        FROM fact_candles f
        LEFT JOIN dim_companies c ON f.company_id = c.id
        LEFT JOIN dim_time t ON f.time_id = t.id
        WHERE c.id IS NULL OR t.id IS NULL
    """,
    "duplicate news-company grain": """
        SELECT COUNT(*) FROM (
            SELECT new_id, company_id FROM fact_news_companies
            GROUP BY new_id, company_id HAVING COUNT(*) > 1
        )
    """,
    "orphan news-company foreign key": """
        SELECT COUNT(*)
        FROM fact_news_companies f
        LEFT JOIN dim_news n ON f.new_id = n.id
        LEFT JOIN dim_companies c ON f.company_id = c.id
        WHERE n.id IS NULL OR c.id IS NULL
    """,
    "invalid news-company score": """
        SELECT COUNT(*) FROM fact_news_companies
        WHERE relevance_score < 0 OR relevance_score > 1
           OR ticker_sentiment_score < -1 OR ticker_sentiment_score > 1
    """,
    "duplicate news-topic grain": """
        SELECT COUNT(*) FROM (
            SELECT new_id, topic_id FROM fact_news_topics
            GROUP BY new_id, topic_id HAVING COUNT(*) > 1
        )
    """,
    "orphan news-topic foreign key": """
        SELECT COUNT(*)
        FROM fact_news_topics f
        LEFT JOIN dim_news n ON f.new_id = n.id
        LEFT JOIN dim_topics t ON f.topic_id = t.id
        WHERE n.id IS NULL OR t.id IS NULL
    """,
    "invalid news-topic score": """
        SELECT COUNT(*) FROM fact_news_topics
        WHERE relevance_score < 0 OR relevance_score > 1
    """,
}


def validate_dwh(db_path: str | None = None, **_) -> None:
    path = db_path or DWH_PATH
    failures = []
    with duckdb.connect(path, read_only=True) as conn:
        for name, query in QUALITY_CHECKS.items():
            invalid_rows = conn.execute(query).fetchone()[0]
            print(f"[data-quality] {name}: {invalid_rows} invalid row(s)")
            if invalid_rows:
                failures.append(f"{name}={invalid_rows}")

    if failures:
        raise RuntimeError("Data quality validation failed: " + ", ".join(failures))
    print("All warehouse data-quality checks passed.")


if __name__ == "__main__":
    validate_dwh()
