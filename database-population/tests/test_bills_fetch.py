"""Unit tests for paging, the per-run page cap, and rate limits in bills.fetch_updates (no DB or API)."""

import logging

import pytest

from snapshots import bills

WATERMARK = "2026-10-01T00:00:00"


@pytest.fixture
def fake_api(monkeypatch):
    """Fake Openstates with num_pages pages of one bill each; optionally HTTP 429 at a page.

    Returns the list of (page, updated_since) requests made.
    """
    calls = []

    def install(num_pages, rate_limit_at=None):
        def get_bill_data(page=1, updated_since=None):
            calls.append((page, updated_since))
            if page == rate_limit_at:
                raise bills.openstates.RateLimitExceeded(f"HTTP 429 on page {page}")
            bill = [f"ocd-bill/{page}"] + [""] * (len(bills.BILL_COLUMNS) - 1)
            data = {
                "bills": [bill],
                "bill_actions": [],
                "bill_sponsors": [],
                "bill_votes": [],
            }
            return data, num_pages

        monkeypatch.setattr(bills.openstates, "get_bill_data", get_bill_data)
        return calls

    return install


def pages_requested(calls):
    return [page for page, _ in calls]


def test_stops_at_max_page_and_warns(fake_api, caplog):
    calls = fake_api(num_pages=485)

    with caplog.at_level(logging.WARNING, logger=bills.__name__):
        result = bills.fetch_updates(WATERMARK, max_page=3)

    # Every page request carries the same updated_since watermark
    assert calls == [(1, WATERMARK), (2, WATERMARK), (3, WATERMARK)]
    assert len(result["bills"]) == 3
    assert (result["pages_fetched"], result["pages_total"]) == (3, 485)
    assert "capped at page 3 of 485" in caplog.text


def test_stops_at_last_page_without_warning(fake_api, caplog):
    calls = fake_api(num_pages=2)

    with caplog.at_level(logging.WARNING, logger=bills.__name__):
        result = bills.fetch_updates(WATERMARK, max_page=200)

    assert pages_requested(calls) == [1, 2]
    assert len(result["bills"]) == 2
    assert (result["pages_fetched"], result["pages_total"]) == (2, 2)
    assert "capped" not in caplog.text


def test_rate_limit_keeps_fetched_pages(fake_api, caplog):
    calls = fake_api(num_pages=485, rate_limit_at=3)

    with caplog.at_level(logging.WARNING, logger=bills.__name__):
        result = bills.fetch_updates(WATERMARK, max_page=200)

    assert pages_requested(calls) == [1, 2, 3]
    assert len(result["bills"]) == 2
    assert (result["pages_fetched"], result["pages_total"]) == (2, 485)
    assert result["rate_limited"] is True
    assert "rate limit reached" in caplog.text
    assert "capped" not in caplog.text


def test_rate_limit_on_first_page_returns_empty(fake_api):
    fake_api(num_pages=485, rate_limit_at=1)

    result = bills.fetch_updates(WATERMARK, max_page=200)

    assert len(result["bills"]) == 0
    assert (result["pages_fetched"], result["pages_total"]) == (0, None)
    assert result["rate_limited"] is True
