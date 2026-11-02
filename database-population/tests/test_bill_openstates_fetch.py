"""Unit tests for Openstates HTTP error handling in fetch_bill_batch (no network)."""

import json

import pytest
import requests
from tenacity import RetryError, wait_none

import sources.bill_openstates_fetch as openstates

SECRET_URL = "https://v3.openstates.org/bills?page=1&apikey=SECRET-KEY"


def make_response(status, body):
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(body).encode()
    response.url = SECRET_URL
    return response


@pytest.fixture
def fake_get(monkeypatch):
    monkeypatch.setattr(openstates, "sleep", lambda _: None)

    def install(*responses):
        calls = []

        def get(url, params):
            calls.append(params["page"])
            return responses[min(len(calls), len(responses)) - 1]

        monkeypatch.setattr(openstates.requests, "get", get)
        return calls

    return install


# Same retry policy as production, without the backoff waits
fetch = openstates.fetch_bill_batch.retry_with(wait=wait_none())


def test_429_raises_rate_limit_without_retry(fake_get):
    calls = fake_get(make_response(429, {"detail": "exceeded limit of 250/day: 257"}))

    with pytest.raises(openstates.RateLimitExceeded) as exc:
        fetch(1, "2026-10-27T17:39:04")

    assert calls == [1]
    assert "SECRET-KEY" not in str(exc.value)


def test_5xx_is_retried_then_succeeds(fake_get):
    ok = make_response(200, {"results": [], "pagination": {"max_page": 4}})
    calls = fake_get(make_response(503, {}), make_response(502, {}), ok)

    results, num_pages = fetch(1, "2026-10-27T17:39:04")

    assert calls == [1, 1, 1]
    assert (results, num_pages) == ([], 4)


def test_5xx_exhausts_retries_without_leaking_key(fake_get):
    calls = fake_get(make_response(504, {}))

    with pytest.raises(RetryError) as exc:
        fetch(1, "2026-10-27T17:39:04")

    assert len(calls) == 3
    error = exc.value.last_attempt.exception()
    assert isinstance(error, requests.exceptions.HTTPError)
    assert "SECRET-KEY" not in str(error)
