from scripts.common.config import DATA_ROOT
from scripts.common.files import dated_file
from scripts.common.storage import upload_parquet


def upload_daily_artifacts(**context) -> None:
    execution_date = context["execution_date"]
    for dataset in ("companies", "news", "ohlcs"):
        path = dated_file(
            DATA_ROOT / "parquet" / dataset,
            f"crawl_{dataset}",
            execution_date,
            ".parquet",
        )
        if not path.exists():
            raise FileNotFoundError(f"Expected daily artifact does not exist: {path}")
        upload_parquet(path, dataset, execution_date)
