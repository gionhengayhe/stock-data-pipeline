import sys
from datetime import datetime

from pyspark.sql import SparkSession

from scripts.common.config import required_env
from scripts.common.storage import parquet_key


def create_spark_session(app_name: str) -> SparkSession:
    """Create a Spark session without coupling the job to one deployment mode."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", required_env("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", required_env("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint.region", required_env("AWS_REGION"))
        .getOrCreate()
    )


def s3a_parquet_uri(dataset: str, execution_date) -> str:
    bucket = required_env("BUCKET_NAME")
    return f"s3a://{bucket}/{parquet_key(dataset, execution_date)}"


def execution_date_argument() -> datetime:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: spark-submit <job.py> YYYY-MM-DD")
    return datetime.strptime(sys.argv[1], "%Y-%m-%d")
