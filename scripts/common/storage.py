from pathlib import Path

import boto3

from scripts.common.config import required_env
from scripts.common.files import date_token


def s3_client():
    return boto3.client("s3", region_name=required_env("AWS_REGION"))


def parquet_key(dataset: str, execution_date) -> str:
    return f"{dataset}/crawl_{dataset}-{date_token(execution_date)}.parquet"


def upload_parquet(path: str | Path, dataset: str, execution_date) -> str:
    bucket = required_env("BUCKET_NAME")
    key = parquet_key(dataset, execution_date)
    s3_client().upload_file(str(path), bucket, key)
    print(f"Uploaded {path} to s3://{bucket}/{key}")
    return key
