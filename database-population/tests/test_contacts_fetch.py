"""Unit tests for skipping failed Capitol Codex scrapes in contacts.fetch_updates (no network)."""

import logging

import pytest

from snapshots import contacts


@pytest.fixture
def scraper_failing_for(monkeypatch):
    """Make extract_contacts raise for the given chambers and return no contacts for the rest."""

    def install(*failing_chambers):
        def extract_contacts(chamber):
            if chamber in failing_chambers:
                raise TypeError("bad operand type for unary ~: 'float'")
            return {}

        monkeypatch.setattr(contacts.codex, "extract_contacts", extract_contacts)

    return install


@pytest.mark.parametrize(
    "failing, expected",
    [
        (["asm"], {"upper": {}}),
        (["sen"], {"lower": {}}),
        (["asm", "sen"], {}),
    ],
)
def test_failed_chamber_is_skipped_with_warning(scraper_failing_for, caplog, failing, expected):
    scraper_failing_for(*failing)

    with caplog.at_level(logging.WARNING, logger=contacts.logger.name):
        assert contacts.fetch_updates() == expected

    for chamber in failing:
        assert f"Skipping Capitol Codex scrape for {chamber}" in caplog.text
