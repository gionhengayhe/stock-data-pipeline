# 📊 Financial Market Data Engineering Pipeline

## 🔍 Overview

A production-grade batch ETL pipeline for financial data, integrating APIs, automated processing, and dashboarding—all containerized for portability and scalability.

## 🎯 Problem Statement

Financial data is fragmented across APIs, hard to query at scale, and often costly to clean and structure. This leads to inefficiency for analysts and decision-makers.

This project addresses the problem by:

- Consolidating data from **three separate APIs** (stock prices, news sentiment, company metadata)
- Processing over **100,000+ records**
- Automating foreign key mapping and schema normalization
- Reducing manual data preparation time by over **85%**
- Powering manually refreshed **Tableau** dashboards for data storytelling

---

## ⚙️ Architecture

- **Orchestration:** Apache Airflow
- **Database Backend:** PostgreSQL
- **Data Lake:** Amazon S3
- **Data Warehouse:** DuckDB
- **Processing Engine:** PySpark (containerized)
- **Visualization:** Tableau
- **CI/CD**: Docker

---

## 📡 Data Sources

| Source API                                   | Description                                                                                                                     |
| -------------------------------------------- |---------------------------------------------------------------------------------------------------------------------------------|
| [sec-api.io](https://sec-api.io)             | Provides the list of companies currently listed on major U.S. stock exchanges (NYSE, NASDAQ).                                   |
| [Alpha Vantage](https://www.alphavantage.co) | Offers two APIs: one for global market status, and another for financial news with sentiment scores from top-tier news sources. |
| [Polygon.io](https://polygon.io)             | Daily OHLCV per company                                                                                                         |

---

## 🧱 Data Warehouse Design

The warehouse uses a **Galaxy Schema**, with multiple fact tables sharing conformed dimensions:

### Fact Tables

- `fact_news_companies`: Links news articles to companies.
- `fact_news_topics`: Links news articles to topics.
- `fact_candles`: OHLCV market prices per company per day.

### Dimension Tables

- `dim_news`: News metadata + sentiment scores.
- `dim_companies`: Company name, ticker, and stock exchange status.
- `dim_topics`: Extracted topics using NLP.
- `dim_time`: Daily calendar dimension (one row per date).

---

## 📁 Project Structure

```bash
.
├── airflow/                # Airflow DAGs and configs
├── data/                   # Data files
├── database/
│   ├── config_dwh/         # DDL for DuckDB schema
│   └── config_db/          # DDL for PostgreSQL schema
├── scripts/                # Python + PySpark script
├── notebooks/              # Jupyter notebooks for analysis
├── flask-api/              # Flask API for serving data
├── README.md
├── docker-compose.yml      # Docker Compose for local dev
```

## 🚀 Getting Started

```bash
git clone https://github.com/gionhengayhe/stock-data-pipeline.git
cd stock-data-pipeline
docker-compose up --build
```
Access Airflow UI at: http://localhost:8080

Access Flask API at: http://localhost:5000
## 📎 Deliverables

- 📘 Project Documentation: [View Detailed Documentation](https://drive.google.com/file/d/12CdgVoBiFTVqCraiy8LkM63DVi-YIiqu/view?usp=drive_link)

- 📊 Interactive Tableau Dashboard: [View Dashboard](https://drive.google.com/file/d/1SqiWW-mO0_QiTjexGpnsXwUH5cBxHKYc/view?usp=drive_link)
