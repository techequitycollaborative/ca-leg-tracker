"""Unit tests for the per-run page cap in bills.fetch_updates (no DB, no API)."""

import logging

import pytest

from snapshots import bills


@pytest.fixture
def fake_api(monkeypatch):
    calls = []

    def install(num_pages):
        def get_bill_data(page=1, updated_since=None):
            calls.append((page, updated_since))
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


def test_stops_at_max_page_and_warns(fake_api, caplog):
    calls = fake_api(num_pages=485)

    with caplog.at_level(logging.WARNING, logger=bills.__name__):
        result = bills.fetch_updates("2026-10-01T00:00:00", max_page=3)

    assert [page for page, _ in calls] == [1, 2, 3]
    assert len(result["bills"]) == 3
    assert (result["pages_fetched"], result["pages_total"]) == (3, 485)
    assert "capped at page 3 of 485" in caplog.text


def test_stops_at_last_page_without_warning(fake_api, caplog):
    calls = fake_api(num_pages=2)

    with caplog.at_level(logging.WARNING, logger=bills.__name__):
        result = bills.fetch_updates("2026-10-01T00:00:00", max_page=200)

    assert [page for page, _ in calls] == [1, 2]
    assert len(result["bills"]) == 2
    assert (result["pages_fetched"], result["pages_total"]) == (2, 2)
    assert "capped" not in caplog.text


def test_forwards_watermark_to_every_page(fake_api):
    calls = fake_api(num_pages=5)

    bills.fetch_updates("2026-10-01T00:00:00", max_page=bills.MAX_PAGES_PER_RUN)

    assert len(calls) == 5
    assert {since for _, since in calls} == {"2026-10-01T00:00:00"}
