from __future__ import annotations

import socket
from contextlib import contextmanager
from threading import Lock
from typing import Any, Iterator

import requests
import urllib3.util.connection as urllib3_connection
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_TIMEOUT_SECONDS = 30
_IPV6_OVERRIDE_LOCK = Lock()


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


@contextmanager
def _force_ipv6_resolution(enabled: bool) -> Iterator[None]:
    """Temporarily force urllib3 connections to resolve IPv6 addresses only."""
    if not enabled:
        yield
        return

    # urllib3 uses allowed_gai_family() before socket.getaddrinfo().
    # The override is process-global, so guard it while the request is running.
    with _IPV6_OVERRIDE_LOCK:
        original = urllib3_connection.allowed_gai_family
        urllib3_connection.allowed_gai_family = lambda: socket.AF_INET6
        try:
            yield
        finally:
            urllib3_connection.allowed_gai_family = original


def get_json(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    required_key: str | None = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    force_ipv6: bool = False,
) -> Any:
    """Fetch JSON without logging query parameters that may contain API keys."""
    with _force_ipv6_resolution(force_ipv6):
        with build_session() as session:
            response = session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            payload = response.json()

    if required_key and required_key not in payload:
        message = (
            payload.get("Information")
            or payload.get("Note")
            or payload.get("Error Message")
            or payload.get("error")
        )
        detail = f": {message}" if message else ""
        raise RuntimeError(
            f"Expected key '{required_key}' was missing from API response{detail}"
        )
    return payload
