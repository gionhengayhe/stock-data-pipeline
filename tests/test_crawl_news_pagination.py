from datetime import datetime

import pytest

from scripts.elt_to_dwh.extract import crawl_news


def _article(name: str, published_at: str) -> dict:
    return {
        "title": name,
        "url": f"https://example.com/{name}",
        "time_published": published_at,
    }


def test_fetch_news_uses_latest_timestamp_cursor_and_deduplicates(monkeypatch):
    pages = [
        [
            _article("a", "20260801T120000"),
            _article("b", "20260801T110000"),
            _article("c", "20260801T103000"),
        ],
        [
            _article("c", "20260801T103000"),
            _article("d", "20260801T090000"),
            _article("e", "20260801T080000"),
        ],
        [
            _article("e", "20260801T080000"),
            _article("f", "20260801T070000"),
        ],
    ]
    requested_params = []

    def fake_get_json(_url, *, params, required_key, force_ipv6):
        requested_params.append(params)
        assert required_key == "feed"
        assert force_ipv6 is True
        return {"feed": pages[len(requested_params) - 1]}

    monkeypatch.setattr(crawl_news, "API_LIMIT", 3)
    monkeypatch.setattr(crawl_news, "get_json", fake_get_json)

    result = crawl_news._fetch_news_range(
        datetime(2026, 8, 1, 0, 0),
        datetime(2026, 8, 1, 23, 59),
        "test-key",
    )

    assert [row["title"] for row in result] == ["a", "b", "c", "d", "e", "f"]
    assert [params["time_to"] for params in requested_params] == [
        "20260801T2359",
        "20260801T1030",
        "20260801T0800",
    ]
    assert all(params["sort"] == "LATEST" for params in requested_params)
    assert all(params["time_from"] == "20260801T0000" for params in requested_params)


def test_fetch_news_fails_when_a_full_page_cannot_advance(monkeypatch):
    def fake_get_json(_url, *, params, required_key, force_ipv6):
        assert force_ipv6 is True
        return {
            "feed": [
                _article("a", "20260801T235959"),
                _article("b", "20260801T235900"),
            ]
        }

    monkeypatch.setattr(crawl_news, "API_LIMIT", 2)
    monkeypatch.setattr(crawl_news, "get_json", fake_get_json)

    with pytest.raises(RuntimeError, match="cursor pagination cannot continue"):
        crawl_news._fetch_news_range(
            datetime(2026, 8, 1, 0, 0),
            datetime(2026, 8, 1, 23, 59),
            "test-key",
        )
