# Stock Data Pipeline

An end-to-end batch data engineering project that collects U.S. market and
financial-news data, transforms it with Apache Spark, loads a dimensional DuckDB
warehouse, and delivers an interactive Tableau dashboard.

![Stock Data Pipeline market overview dashboard](dashboard/01.%20Market%20Overview.png)

## Project Overview

Airflow orchestrates two containerized workflows:

- `etl_to_db` refreshes company and market metadata in PostgreSQL each month.
- `elt_to_dwh` collects daily OHLCV and news data, stages Parquet files in Amazon
  S3, runs Spark transformations, loads DuckDB, and executes the quality gate.

The Flask API exposes the four dimensions and three fact tables. A snapshot-safe
export writes those tables unchanged to
`flask-api/exported_data/stock_data.xlsx`, which Tableau consumes through table
relationships and calculated fields.

## Why This Project Matters

The goal is not only to collect stock data. It is to turn a fragile sequence of
API calls and scripts into one explainable path from source to analysis:

- **Less repetitive work:** Airflow coordinates the 11 daily tasks that would
  otherwise need to be started and checked in the correct order. A refresh is
  estimated to require **70–80% less active operator time**—around 10–15 minutes
  of monitoring instead of 45–60 minutes of manual operation.
- **Safer data delivery:** retries isolate operational failures, while the quality
  gate checks schema, freshness, volume, date coverage, uniqueness, and foreign
  keys before the warehouse is used downstream.
- **Clear system boundaries:** PostgreSQL stores source metadata, S3 and Parquet
  provide the staging layer, Spark owns transformation, DuckDB serves the
  dimensional warehouse, and Flask provides a read-only delivery interface.
- **Reusable analytics model:** Tableau works directly with the dimension and fact
  tables through relationships and calculated fields. The pipeline does not
  contain dashboard-specific transformations, so the warehouse remains reusable
  beyond the current workbook.

The time estimate describes human effort rather than API or Spark execution time.

## Key Findings

*Scope: 10–31 August 2026 · NYSE and NASDAQ exchange metadata · 16 market
sessions.*

1. **Market breadth was weak across the window.** Only **41.8%** of company-day
   observations advanced, compared with **49.8%** that declined. No observed
   session had more than half of companies advancing; 28 August was the weakest,
   at **27.1% advancers** and a **-0.43% median intraday return**.
2. **Trading activity was highly concentrated.** Technology generated **37.7% of
   estimated trading value** while representing **11.1% of observed companies**
   (about **3.4×** its company share). The top 10 tickers contributed another
   concentration signal: **22.5% of total estimated trading value**.
3. **News sentiment was not a useful same-day trading signal.** Ticker sentiment
   and same-day intraday return had only **0.059 Pearson correlation** across
   **27,578 eligible company-days**, so sentiment is presented as market context,
   not as evidence of price direction or causation.

The calculations are reproducible in
[`notebooks/findings_analysis.ipynb`](notebooks/findings_analysis.ipynb).

## Architecture

### Data Pipeline and Taskflow

![Data pipeline and Airflow taskflow](docs/diagrams/data-pipeline-taskflow.svg)

### Dimensional Warehouse

![DuckDB dimensional warehouse ERD](docs/diagrams/warehouse-erd.svg)

### Pipeline Tools

![Technology stack and data flow](docs/diagrams/pipeline-tools.svg)

## Dashboard Pages

The Tableau workbook contains six pages for market overview, breadth, sector,
company, news-sentiment, and article exploration.

<details>
<summary>View the remaining dashboard pages</summary>

![Market breadth](dashboard/02.%20Market%20Breadth.png)

![Sector analysis](dashboard/03.%20Sector%20Analysis.png)

![Company explorer](dashboard/04.%20Company%20Explorer.png)

![News sentiment](dashboard/05.%20News%20Sentiment.png)

![Topics and articles](dashboard/06.%20Topics%20%26%20Articles.png)

</details>

## Project Structure

```text
airflow/        Airflow DAGs and image configuration
database/       PostgreSQL and DuckDB schemas
scripts/        Extraction, staging, Spark transformation, and quality checks
flask-api/      Read-only API and Excel exporter
dashboard/      Tableau workbook and dashboard previews
notebooks/      Reproducible findings analysis
docs/           Architecture diagrams and learning notes
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

### 3. Export data and open Tableau

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

Estimated trading value is calculated as volume × volume-weighted price. This is
a portfolio learning project, not a live trading system or investment advice.
