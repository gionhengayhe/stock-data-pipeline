import json
import os
import tempfile
from typing import Any


def write_json_atomic(data: Any, file_path: str) -> None:
    """Write JSON atomically so downstream tasks never read a partial file."""
    directory = os.path.dirname(file_path)
    os.makedirs(directory, exist_ok=True)
    descriptor, temp_path = tempfile.mkstemp(prefix=".tmp-", suffix=".json", dir=directory)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(data, stream, ensure_ascii=False, indent=2)
        os.replace(temp_path, file_path)
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise
