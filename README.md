# Stock Data Pipeline

Containerized batch data pipeline that collects U.S. stock-market and financial
news data, processes it with Apache Spark, loads it into a dimensional DuckDB
warehouse, and serves it to a Tableau dashboard.

## What It Does

```text
SEC API / Alpha Vantage / Polygon.io
                ↓
        Airflow orchestration
                ↓
      Parquet → Amazon S3
                ↓
       Apache Spark transforms
                ↓
       DuckDB + quality gate
                ↓
      Flask API → Excel → Tableau
```

The project contains two Airflow workflows:

- `etl_to_db`: refreshes company and market metadata in PostgreSQL monthly.
- `elt_to_dwh`: collects daily OHLCV and news data, runs Spark transformations,
  loads DuckDB, and validates the warehouse.

## Results

- Dimensional warehouse with four dimensions and three fact tables.
- Automated schema, freshness, volume, coverage, uniqueness, and foreign-key
  checks.
- Snapshot-consistent export of raw dimension/fact tables to
  `flask-api/exported_data/stock_data.xlsx`.
- Tableau workbook using relationships and calculated fields directly on the
  dimensional model.
- Reproducible findings notebook and automated pipeline tests.

Current analytical findings include:

- On the latest observed market session, **27.1% of securities advanced** and
  median open-to-close return was **-0.43%**.
- Technology generated **39.1% of estimated trading value** from **11.3% of
  observed securities**.
- Same-day ticker sentiment and return had only **0.069 correlation**, so news
  sentiment is treated as context rather than a trading signal.

## Tech Stack

`Python` · `Polars` · `PostgreSQL` · `Amazon S3` · `Apache Spark` · `DuckDB` ·
`Apache Airflow` · `Flask` · `Docker` · `Tableau`

## Project Structure

```text
airflow/        Airflow DAGs and image configuration
database/       PostgreSQL and DuckDB schemas
scripts/        Extraction, staging, Spark transformation, and quality checks
flask-api/      Read-only API and Excel exporter
dashboard/      Tableau workbook
notebooks/      Reproducible findings analysis
tests/          Pipeline and data-contract tests
```

## How to Run

### 1. Configure credentials

```powershell
Copy-Item .env.example .env
```

Add the required SEC API, Alpha Vantage, Polygon.io, AWS, and S3 credentials to
`.env`.

### 2. Start the pipeline

```powershell
docker compose up -d --build
```

Open Airflow at <http://localhost:8080>. Run `etl_to_db` once before triggering
`elt_to_dwh` so the company metadata snapshot is available.

### 3. Export data for Tableau

```powershell
uv venv --python 3.12 .venv
uv pip install --python .venv\Scripts\python.exe -r requirements-dev.txt
.\.venv\Scripts\python.exe flask-api\dump_api_to_excel.py
```

Open [`dashboard/dashboard.twb`](dashboard/dashboard.twb) and refresh its Excel
data source.

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

This is a portfolio learning project, not a live trading system or investment
recommendation.
