from __future__ import annotations

from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_TIMEOUT_SECONDS = 30


def build_session() -> requests.Session:
    """Create an HTTP session with bounded retries for transient failures."""
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_json(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    required_key: str | None = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> Any:
    """Fetch JSON without logging query parameters that may contain API keys."""
    with build_session() as session:
        response = session.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()

    if required_key and required_key not in payload:
        message = payload.get("Information") or payload.get("Note") or payload.get("error")
        detail = f": {message}" if message else ""
        raise RuntimeError(f"Expected key '{required_key}' was missing from API response{detail}")
    return payload
