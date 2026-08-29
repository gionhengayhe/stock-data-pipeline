import duckdb
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col, explode, lit, to_timestamp
from pyspark.sql.types import DoubleType

from scripts.common.config import duckdb_path
from scripts.common.spark import (
    create_spark_session,
    execution_date_argument,
    s3a_parquet_uri,
)
from scripts.common.warehouse import ensure_time, register_spark_frame, transaction


def transform_topics(news: DataFrame) -> DataFrame:
    return (
        news.select(explode(col("topics")).alias("topic_data"))
        .select(col("topic_data.topic").alias("name"))
        .where(col("name").isNotNull())
        .distinct()
    )


def transform_news(news: DataFrame, time_id: int) -> DataFrame:
    return (
        news.withColumn(
            "time_published",
            to_timestamp(col("time_published"), "yyyyMMdd'T'HHmmss"),
        )
        .withColumn("time_id", lit(time_id))
        .select(
            "title",
            "url",
            "time_published",
            "authors",
            "summary",
            "source",
            "overall_sentiment_score",
            "overall_sentiment_label",
            "time_id",
        )
        .dropDuplicates(["url"])
    )


def transform_news_topics(
    news: DataFrame, news_ids: DataFrame, topic_ids: DataFrame
) -> DataFrame:
    return (
        news.select("url", explode(col("topics")).alias("topic_data"))
        .select(
            "url",
            col("topic_data.topic").alias("name"),
            col("topic_data.relevance_score")
            .cast(DoubleType())
            .alias("relevance_score"),
        )
        .dropna(subset=["url", "name", "relevance_score"])
        .join(broadcast(topic_ids), on="name", how="inner")
        .join(broadcast(news_ids), on="url", how="inner")
        .select("new_id", "topic_id", "relevance_score")
        .dropDuplicates(["new_id", "topic_id"])
    )


def transform_news_companies(
    news: DataFrame, news_ids: DataFrame, company_ids: DataFrame
) -> DataFrame:
    return (
        news.select("url", explode(col("ticker_sentiment")).alias("sentiment"))
        .select(
            "url",
            col("sentiment.ticker").alias("ticker"),
            col("sentiment.relevance_score")
            .cast(DoubleType())
            .alias("relevance_score"),
            col("sentiment.ticker_sentiment_score")
            .cast(DoubleType())
            .alias("ticker_sentiment_score"),
            col("sentiment.ticker_sentiment_label").alias(
                "ticker_sentiment_label"
            ),
        )
        .dropna(
            subset=[
                "url",
                "ticker",
                "relevance_score",
                "ticker_sentiment_score",
            ]
        )
        .join(broadcast(company_ids), on="ticker", how="inner")
        .join(broadcast(news_ids), on="url", how="inner")
        .select(
            "new_id",
            "company_id",
            "ticker_sentiment_score",
            "ticker_sentiment_label",
            "relevance_score",
        )
        .dropDuplicates(["new_id", "company_id"])
    )


def process_news() -> None:
    execution_date = execution_date_argument()
    spark = create_spark_session("process_news")
    source = None
    try:
        source = spark.read.parquet(s3a_parquet_uri("news", execution_date)).cache()
        if source.isEmpty():
            print(f"No news data for {execution_date.date()}")
            return

        with duckdb.connect(duckdb_path()) as conn, transaction(conn):
            topics = transform_topics(source)
            register_spark_frame(conn, "incoming_topics", topics)
            conn.execute(
                """
                INSERT INTO dim_topics (name)
                SELECT name FROM incoming_topics
                ON CONFLICT (name) DO NOTHING
                """
            )

            time_id = ensure_time(conn, execution_date)
            daily_news = transform_news(source, time_id)
            row_count = register_spark_frame(conn, "incoming_news", daily_news)
            conn.execute(
                """
                INSERT INTO dim_news (
                    title, url, time_published, authors, summary, source,
                    overall_sentiment_score, overall_sentiment_label, time_id
                )
                SELECT title, url, time_published, authors, summary, source,
                       overall_sentiment_score, overall_sentiment_label, time_id
                FROM incoming_news
                ON CONFLICT (url) DO NOTHING
                """
            )

            news_ids = spark.createDataFrame(
                conn.execute("SELECT id AS new_id, url FROM dim_news").fetchdf()
            )
            topic_ids = spark.createDataFrame(
                conn.execute("SELECT id AS topic_id, name FROM dim_topics").fetchdf()
            )
            company_ids = spark.createDataFrame(
                conn.execute(
                    """
                    SELECT id AS company_id, ticker
                    FROM (
                        SELECT id, ticker,
                               ROW_NUMBER() OVER (
                                   PARTITION BY ticker
                                   ORDER BY is_delisted ASC,
                                            updated_time DESC NULLS LAST,
                                            id DESC
                               ) AS row_num
                        FROM dim_companies
                    )
                    WHERE row_num = 1
                    """
                ).fetchdf()
            )

            news_topics = transform_news_topics(source, news_ids, topic_ids)
            register_spark_frame(conn, "incoming_news_topics", news_topics)
            conn.execute(
                """
                INSERT INTO fact_news_topics (new_id, topic_id, relevance_score)
                SELECT new_id, topic_id, relevance_score FROM incoming_news_topics
                ON CONFLICT (new_id, topic_id) DO UPDATE SET
                    relevance_score = EXCLUDED.relevance_score
                """
            )

            news_companies = transform_news_companies(
                source, news_ids, company_ids
            )
            register_spark_frame(conn, "incoming_news_companies", news_companies)
            conn.execute(
                """
                INSERT INTO fact_news_companies (
                    new_id, company_id, ticker_sentiment_score,
                    ticker_sentiment_label, relevance_score
                )
                SELECT new_id, company_id, ticker_sentiment_score,
                       ticker_sentiment_label, relevance_score
                FROM incoming_news_companies
                ON CONFLICT (new_id, company_id) DO UPDATE SET
                    ticker_sentiment_score = EXCLUDED.ticker_sentiment_score,
                    ticker_sentiment_label = EXCLUDED.ticker_sentiment_label,
                    relevance_score = EXCLUDED.relevance_score
                """
            )
        print(f"Loaded {row_count} news rows into DuckDB")
    finally:
        if source is not None:
            source.unpersist()
        spark.stop()


if __name__ == "__main__":
    process_news()
