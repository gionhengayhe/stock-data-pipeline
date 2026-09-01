import os
from hashlib import sha256
from pathlib import Path

import duckdb
from flask import Flask, abort, jsonify, request


app = Flask(__name__)
DWH_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/database/config_dwh/mydb.duckdb")
ALLOWED_TABLES = {
    "dim_time",
    "dim_news",
    "dim_topics",
    "dim_companies",
    "fact_news_companies",
    "fact_news_topics",
    "fact_candles",
}
TABLE_ORDER_BY = {
    "dim_time": "date, id",
    "dim_news": "time_id, time_published, id",
    "dim_topics": "name, id",
    "dim_companies": "ticker, is_delisted, id",
    "fact_news_companies": "new_id, company_id, id",
    "fact_news_topics": "new_id, topic_id, id",
    "fact_candles": "time_id, company_id, id",
}
DEFAULT_PAGE_SIZE = 1000
MAX_PAGE_SIZE = 5000


def _pagination() -> tuple[int, int]:
    try:
        limit = int(request.args.get("limit", DEFAULT_PAGE_SIZE))
        offset = int(request.args.get("offset", 0))
    except ValueError:
        abort(400, description="limit and offset must be integers")
    if limit < 1 or limit > MAX_PAGE_SIZE or offset < 0:
        abort(
            400,
            description=(
                f"limit must be 1-{MAX_PAGE_SIZE}; offset must be non-negative"
            ),
        )
    return limit, offset


def _snapshot_token() -> str:
    """Identify the exact DuckDB file version used by a paginated export."""
    database_path = Path(DWH_PATH)
    fingerprints = []
    for path in (database_path, Path(f"{database_path}.wal")):
        if path.exists():
            stat = path.stat()
            fingerprints.append(f"{path.name}:{stat.st_size}:{stat.st_mtime_ns}")
        else:
            fingerprints.append(f"{path.name}:missing")
    fingerprint = "|".join(fingerprints).encode()
    return sha256(fingerprint).hexdigest()


def _require_snapshot(expected_token: str | None) -> str:
    current_token = _snapshot_token()
    if expected_token is not None and expected_token != current_token:
        abort(
            409,
            description="warehouse changed during export; restart from /snapshot",
        )
    return current_token


@app.get("/health")
def health():
    try:
        with duckdb.connect(DWH_PATH, read_only=True) as conn:
            conn.execute("SELECT 1")
        return jsonify({"status": "ok"})
    except Exception as exc:
        return jsonify({"status": "unavailable", "detail": str(exc)}), 503


@app.get("/snapshot")
def snapshot():
    """Return the immutable token and data watermark for one export attempt."""
    token_before = _require_snapshot(None)
    with duckdb.connect(DWH_PATH, read_only=True) as conn:
        snapshot_date = conn.execute(
            """
            SELECT MAX(date)
            FROM (
                SELECT time.date
                FROM fact_candles AS candle
                JOIN dim_time AS time ON time.id = candle.time_id
                UNION ALL
                SELECT time.date
                FROM dim_news AS news
                JOIN dim_time AS time ON time.id = news.time_id
            ) AS observed_dates
            """
        ).fetchone()[0]
    token_after = _require_snapshot(token_before)
    return jsonify(
        {
            "snapshot_token": token_after,
            "snapshot_date": snapshot_date.isoformat() if snapshot_date else None,
        }
    )


@app.get("/<table_name>")
def get_table(table_name: str):
    if table_name not in ALLOWED_TABLES:
        abort(404)

    snapshot_token = _require_snapshot(request.args.get("snapshot"))
    limit, offset = _pagination()
    where_clause = ""
    parameters = []
    if table_name == "fact_candles" and request.args.get("time_id") is not None:
        try:
            parameters.append(int(request.args["time_id"]))
        except ValueError:
            abort(400, description="time_id must be an integer")
        where_clause = " WHERE time_id = ?"

    parameters.extend([limit, offset])
    order_by = TABLE_ORDER_BY[table_name]
    query = (
        f'SELECT * FROM "{table_name}"{where_clause} '
        f"ORDER BY {order_by} LIMIT ? OFFSET ?"
    )
    with duckdb.connect(DWH_PATH, read_only=True) as conn:
        result = conn.execute(query, parameters).fetchall()
        columns = [desc[0] for desc in conn.description]
    _require_snapshot(snapshot_token)
    response = jsonify([dict(zip(columns, row)) for row in result])
    response.headers["X-Data-Snapshot"] = snapshot_token
    return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
