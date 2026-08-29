import os

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
DEFAULT_PAGE_SIZE = 1000
MAX_PAGE_SIZE = 5000


def _pagination() -> tuple[int, int]:
    try:
        limit = int(request.args.get("limit", DEFAULT_PAGE_SIZE))
        offset = int(request.args.get("offset", 0))
    except ValueError:
        abort(400, description="limit and offset must be integers")
    if limit < 1 or limit > MAX_PAGE_SIZE or offset < 0:
        abort(400, description=f"limit must be 1-{MAX_PAGE_SIZE}; offset must be non-negative")
    return limit, offset


@app.get("/health")
def health():
    try:
        with duckdb.connect(DWH_PATH, read_only=True) as conn:
            conn.execute("SELECT 1")
        return jsonify({"status": "ok"})
    except Exception as exc:
        return jsonify({"status": "unavailable", "detail": str(exc)}), 503


@app.get("/<table_name>")
def get_table(table_name: str):
    if table_name not in ALLOWED_TABLES:
        abort(404)

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
    query = f'SELECT * FROM "{table_name}"{where_clause} LIMIT ? OFFSET ?'
    with duckdb.connect(DWH_PATH, read_only=True) as conn:
        result = conn.execute(query, parameters).fetchall()
        columns = [desc[0] for desc in conn.description]
    return jsonify([dict(zip(columns, row)) for row in result])


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
