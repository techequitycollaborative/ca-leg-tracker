"""Unit tests for pipeline success alert formatting (no Slack calls)."""

import pytest

from utils import slack_bot


@pytest.fixture
def sent(monkeypatch):
    messages = []
    monkeypatch.setattr(
        slack_bot,
        "send_slack_alert",
        lambda message, color="danger": messages.append((message, color)),
    )
    return messages


def test_partial_bill_fetch_reports_progress(sent):
    slack_bot.send_pipeline_success_alert(
        {
            "bills_updated": 2000,
            "bill_pages_fetched": 200,
            "bill_pages_total": 485,
            "bills_max_updated_at": "2026-10-20T12:00:00",
        }
    )

    message, color = sent[0]
    assert color == "warning"
    assert "partial bill fetch" in message
    assert "200 of 485 (285 remaining)" in message
    assert "2026-10-20T12:00:00" in message


def test_complete_bill_fetch_is_success(sent):
    slack_bot.send_pipeline_success_alert(
        {"bills_updated": 50, "bill_pages_fetched": 5, "bill_pages_total": 5}
    )

    message, color = sent[0]
    assert color == "good"
    assert "completed successfully" in message
    assert "5 of 5 (0 remaining)" in message
