import json
import os
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any


def date_token(value: date | datetime | str) -> str:
    if isinstance(value, str):
        return value.replace("-", "")
    return value.strftime("%Y%m%d")


def dated_file(
    directory: str | Path,
    prefix: str,
    execution_date: date | datetime | str,
    suffix: str,
) -> Path:
    return Path(directory) / f"{prefix}-{date_token(execution_date)}{suffix}"


def read_json(path: str | Path) -> Any:
    with Path(path).open("r", encoding="utf-8") as stream:
        return json.load(stream)


def write_json_atomic(data: Any, file_path: str | Path) -> None:
    """Write JSON atomically so downstream tasks never read a partial file."""
    target = Path(file_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_path = tempfile.mkstemp(
        prefix=".tmp-", suffix=".json", dir=target.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(data, stream, ensure_ascii=False, indent=2)
        os.replace(temp_path, target)
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise
