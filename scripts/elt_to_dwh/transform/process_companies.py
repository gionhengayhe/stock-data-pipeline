import duckdb
from pyspark.sql import DataFrame

from scripts.common.config import duckdb_path
from scripts.common.spark import (
    create_spark_session,
    execution_date_argument,
    s3a_parquet_uri,
)
from scripts.common.warehouse import register_spark_frame, transaction


COMPANY_COLUMNS = [
    "name",
    "ticker",
    "is_delisted",
    "category",
    "currency",
    "location",
    "exchange",
    "region",
    "industry",
    "sector",
    "sic_industry",
    "sic_sector",
    "updated_time",
]


def transform_companies(frame: DataFrame) -> DataFrame:
    return frame.select(*COMPANY_COLUMNS).dropDuplicates(["ticker", "is_delisted"])


def process_companies() -> None:
    execution_date = execution_date_argument()
    spark = create_spark_session("process_companies")
    try:
        companies = transform_companies(
            spark.read.parquet(s3a_parquet_uri("companies", execution_date))
        )
        with duckdb.connect(duckdb_path()) as conn, transaction(conn):
            row_count = register_spark_frame(conn, "incoming_companies", companies)
            conn.execute(
                """
                UPDATE dim_companies AS target
                SET name = source.name,
                    category = source.category,
                    currency = source.currency,
                    location = source.location,
                    exchange = source.exchange,
                    region = source.region,
                    industry = source.industry,
                    sector = source.sector,
                    sic_industry = source.sic_industry,
                    sic_sector = source.sic_sector,
                    updated_time = source.updated_time
                FROM incoming_companies AS source
                WHERE target.ticker = source.ticker
                  AND target.is_delisted = source.is_delisted
                """
            )
            conn.execute(
                """
                INSERT INTO dim_companies (
                    name, ticker, is_delisted, category, currency, location,
                    exchange, region, industry, sector, sic_industry, sic_sector,
                    updated_time
                )
                SELECT name, ticker, is_delisted, category, currency, location,
                       exchange, region, industry, sector, sic_industry, sic_sector,
                       updated_time
                FROM incoming_companies AS source
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_companies AS target
                    WHERE target.ticker = source.ticker
                      AND target.is_delisted = source.is_delisted
                )
                """
            )
        print(f"Loaded {row_count} company rows into DuckDB")
    finally:
        spark.stop()


if __name__ == "__main__":
    process_companies()
