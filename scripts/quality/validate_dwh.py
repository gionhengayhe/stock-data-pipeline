import os
from datetime import date, datetime

import duckdb


DWH_PATH = os.getenv(
    "DUCKDB_PATH", "/opt/airflow/database/config_dwh/mydb.duckdb"
)

EXPECTED_SCHEMA = {
    "dim_time": {
        "id": "INTEGER",
        "date": "DATE",
        "day_of_week": "VARCHAR",
        "month": "VARCHAR",
        "quarter": "VARCHAR",
        "year": "INTEGER",
    },
    "dim_companies": {
        "id": "INTEGER",
        "name": "VARCHAR",
        "ticker": "VARCHAR",
        "is_delisted": "BOOLEAN",
        "category": "VARCHAR",
        "currency": "VARCHAR",
        "location": "VARCHAR",
        "exchange": "VARCHAR",
        "region": "VARCHAR",
        "industry": "VARCHAR",
        "sector": "VARCHAR",
        "sic_industry": "VARCHAR",
        "sic_sector": "VARCHAR",
        "updated_time": "TIMESTAMP",
    },
    "dim_topics": {
        "id": "INTEGER",
        "name": "VARCHAR",
    },
    "dim_news": {
        "id": "INTEGER",
        "title": "VARCHAR",
        "url": "VARCHAR",
        "time_published": "TIMESTAMP",
        "authors": "VARCHAR[]",
        "summary": "VARCHAR",
        "source": "VARCHAR",
        "overall_sentiment_score": "DOUBLE",
        "overall_sentiment_label": "VARCHAR",
        "time_id": "INTEGER",
    },
    "fact_candles": {
        "id": "INTEGER",
        "company_id": "INTEGER",
        "volume": "BIGINT",
        "volume_weighted": "DOUBLE",
        "open": "DOUBLE",
        "close": "DOUBLE",
        "high": "DOUBLE",
        "low": "DOUBLE",
        "time_stamp": "TIMESTAMP",
        "num_of_trades": "BIGINT",
        "is_otc": "BOOLEAN",
        "time_id": "INTEGER",
    },
    "fact_news_companies": {
        "id": "INTEGER",
        "company_id": "INTEGER",
        "new_id": "INTEGER",
        "relevance_score": "DOUBLE",
        "ticker_sentiment_score": "DOUBLE",
        "ticker_sentiment_label": "VARCHAR",
    },
    "fact_news_topics": {
        "id": "INTEGER",
        "new_id": "INTEGER",
        "topic_id": "INTEGER",
        "relevance_score": "DOUBLE",
    },
}

MIN_COMPANY_CLASSIFICATION_COVERAGE = 0.80
MIN_NEWS_COMPANY_COVERAGE = 0.70
MIN_NEWS_TOPIC_COVERAGE = 0.95
MIN_CANDLE_VOLUME_RATIO = 0.50
MAX_CANDLE_VOLUME_RATIO = 1.50
MIN_BASELINE_SESSIONS = 3

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
    "null warehouse date key": """
        SELECT
            (SELECT COUNT(*) FROM fact_candles WHERE time_id IS NULL)
          + (SELECT COUNT(*) FROM dim_news WHERE time_id IS NULL)
    """,
    "timestamp-date mismatch": """
        SELECT
            (SELECT COUNT(*)
             FROM fact_candles f
             JOIN dim_time t ON f.time_id = t.id
             WHERE CAST(f.time_stamp AS DATE) <> t.date)
          + (SELECT COUNT(*)
             FROM dim_news n
             JOIN dim_time t ON n.time_id = t.id
             WHERE CAST(n.time_published AS DATE) <> t.date)
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


def _as_date(value: date | datetime | str | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _validate_schema(conn: duckdb.DuckDBPyConnection, failures: list[str]) -> None:
    for table, expected_columns in EXPECTED_SCHEMA.items():
        actual_rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
        actual_columns = {row[1]: row[2].upper() for row in actual_rows}

        if not actual_columns:
            failures.append(f"schema {table}=missing table")
            print(f"[data-quality] schema {table}: missing table")
            continue

        issues = []
        missing = sorted(set(expected_columns) - set(actual_columns))
        unexpected = sorted(set(actual_columns) - set(expected_columns))
        mismatched = [
            f"{column} expected {expected_type}, got {actual_columns[column]}"
            for column, expected_type in expected_columns.items()
            if column in actual_columns
            and actual_columns[column] != expected_type
        ]
        if missing:
            issues.append("missing " + ", ".join(missing))
        if unexpected:
            issues.append("unexpected " + ", ".join(unexpected))
        issues.extend(mismatched)

        if issues:
            detail = "; ".join(issues)
            failures.append(f"schema {table}={detail}")
            print(f"[data-quality] schema {table}: FAILED ({detail})")
        else:
            print(f"[data-quality] schema {table}: passed")


def _validate_freshness(
    conn: duckdb.DuckDBPyConnection,
    expected_date: date | None,
    failures: list[str],
) -> None:
    latest_dim_date = conn.execute("SELECT MAX(date) FROM dim_time").fetchone()[0]
    latest_news_date = conn.execute(
        "SELECT MAX(CAST(time_published AS DATE)) FROM dim_news"
    ).fetchone()[0]
    latest_candle_date = conn.execute(
        "SELECT MAX(CAST(time_stamp AS DATE)) FROM fact_candles"
    ).fetchone()[0]
    print(
        "[data-quality] freshness watermarks: "
        f"dim_time={latest_dim_date}, news={latest_news_date}, "
        f"candles={latest_candle_date}"
    )

    target_date = expected_date or latest_news_date
    if target_date is None:
        failures.append("freshness=no news date available")
        return

    dim_rows = conn.execute(
        "SELECT COUNT(*) FROM dim_time WHERE date = ?", [target_date]
    ).fetchone()[0]
    news_rows = conn.execute(
        """
        SELECT COUNT(*) FROM dim_news
        WHERE CAST(time_published AS DATE) = ?
        """,
        [target_date],
    ).fetchone()[0]
    print(
        f"[data-quality] freshness expected date {target_date}: "
        f"dim_time={dim_rows}, news={news_rows}"
    )
    if dim_rows != 1 or news_rows == 0:
        failures.append(
            f"freshness expected date {target_date}="
            f"dim_time {dim_rows}, news {news_rows}"
        )


def _validate_volume(conn: duckdb.DuckDBPyConnection, failures: list[str]) -> None:
    for table in EXPECTED_SCHEMA:
        row_count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        print(f"[data-quality] volume {table}: {row_count} row(s)")
        if row_count == 0:
            failures.append(f"volume {table}=empty")

    latest_partition = conn.execute(
        """
        SELECT CAST(time_stamp AS DATE) AS partition_date, COUNT(*) AS row_count
        FROM fact_candles
        GROUP BY partition_date
        ORDER BY partition_date DESC
        LIMIT 1
        """
    ).fetchone()
    if latest_partition is None:
        return

    latest_date, latest_count = latest_partition
    baseline_counts = [
        row[0]
        for row in conn.execute(
            """
            SELECT COUNT(*) AS row_count
            FROM fact_candles
            WHERE CAST(time_stamp AS DATE) < ?
            GROUP BY CAST(time_stamp AS DATE)
            ORDER BY CAST(time_stamp AS DATE) DESC
            LIMIT 10
            """,
            [latest_date],
        ).fetchall()
    ]
    if len(baseline_counts) < MIN_BASELINE_SESSIONS:
        print(
            "[data-quality] candle volume ratio: skipped "
            f"({len(baseline_counts)} prior session(s); "
            f"need {MIN_BASELINE_SESSIONS})"
        )
        return

    sorted_counts = sorted(baseline_counts)
    midpoint = len(sorted_counts) // 2
    if len(sorted_counts) % 2:
        baseline_median = float(sorted_counts[midpoint])
    else:
        baseline_median = (
            sorted_counts[midpoint - 1] + sorted_counts[midpoint]
        ) / 2
    volume_ratio = latest_count / baseline_median
    print(
        f"[data-quality] candle volume ratio {latest_date}: "
        f"{volume_ratio:.3f} ({latest_count} vs median {baseline_median:.1f})"
    )
    if not MIN_CANDLE_VOLUME_RATIO <= volume_ratio <= MAX_CANDLE_VOLUME_RATIO:
        failures.append(
            f"candle volume ratio={volume_ratio:.3f} outside "
            f"[{MIN_CANDLE_VOLUME_RATIO:.2f}, {MAX_CANDLE_VOLUME_RATIO:.2f}]"
        )


def _coverage_ratio(
    conn: duckdb.DuckDBPyConnection, query: str
) -> float | None:
    value = conn.execute(query).fetchone()[0]
    return None if value is None else float(value)


def _validate_coverage(
    conn: duckdb.DuckDBPyConnection, failures: list[str]
) -> None:
    checks = {
        "company classification coverage": (
            _coverage_ratio(
                conn,
                """
                SELECT COUNT(*) FILTER (
                    WHERE NULLIF(TRIM(sector), '') IS NOT NULL
                      AND NULLIF(TRIM(industry), '') IS NOT NULL
                )::DOUBLE / NULLIF(COUNT(*), 0)
                FROM dim_companies
                """,
            ),
            MIN_COMPANY_CLASSIFICATION_COVERAGE,
        ),
        "news-company coverage": (
            _coverage_ratio(
                conn,
                """
                SELECT COUNT(DISTINCT f.new_id)::DOUBLE
                       / NULLIF(COUNT(DISTINCT n.id), 0)
                FROM dim_news n
                LEFT JOIN fact_news_companies f ON f.new_id = n.id
                """,
            ),
            MIN_NEWS_COMPANY_COVERAGE,
        ),
        "news-topic coverage": (
            _coverage_ratio(
                conn,
                """
                SELECT COUNT(DISTINCT f.new_id)::DOUBLE
                       / NULLIF(COUNT(DISTINCT n.id), 0)
                FROM dim_news n
                LEFT JOIN fact_news_topics f ON f.new_id = n.id
                """,
            ),
            MIN_NEWS_TOPIC_COVERAGE,
        ),
    }

    for name, (actual, minimum) in checks.items():
        display = "n/a" if actual is None else f"{actual:.3%}"
        print(f"[data-quality] {name}: {display} (minimum {minimum:.0%})")
        if actual is None or actual < minimum:
            failures.append(f"{name}={display}, minimum {minimum:.0%}")


def validate_dwh(
    db_path: str | None = None,
    expected_date: date | datetime | str | None = None,
    **context,
) -> None:
    path = db_path or DWH_PATH
    logical_date = _as_date(
        expected_date or context.get("ds") or context.get("logical_date")
    )
    failures: list[str] = []

    with duckdb.connect(path, read_only=True) as conn:
        _validate_schema(conn, failures)
        if not any(failure.startswith("schema ") for failure in failures):
            for name, query in QUALITY_CHECKS.items():
                invalid_rows = conn.execute(query).fetchone()[0]
                print(f"[data-quality] {name}: {invalid_rows} invalid row(s)")
                if invalid_rows:
                    failures.append(f"{name}={invalid_rows}")

            _validate_freshness(conn, logical_date, failures)
            _validate_volume(conn, failures)
            _validate_coverage(conn, failures)

    if failures:
        raise RuntimeError("Data quality validation failed: " + ", ".join(failures))
    print("All warehouse data-quality checks passed.")


if __name__ == "__main__":
    validate_dwh()
