"""Unit tests for combining Assembly and Senate scrapes in hearings.fetch_updates (no browser or DB)."""

import logging

import pytest

from snapshots import hearings

# Scrapers return (hearings, scheduled_bills); None means the scrape failed
ASM_RESULT = ({("asm-hearing",)}, {("asm-bill",)})
SEN_RESULT = ({("sen-hearing",)}, {("sen-bill",)})
FAILED = None


@pytest.fixture
def scrapers_return(monkeypatch):
    def install(asm_result, sen_result):
        monkeypatch.setattr(hearings.assembly, "scrape_committee_hearing", lambda verbose: asm_result)
        monkeypatch.setattr(hearings.senate, "scrape_committee_hearing", lambda verbose: sen_result)

    return install


def test_combines_both_chambers(scrapers_return):
    scrapers_return(ASM_RESULT, SEN_RESULT)

    assert hearings.fetch_updates() == (
        {("asm-hearing",), ("sen-hearing",)},
        {("asm-bill",), ("sen-bill",)},
    )


@pytest.mark.parametrize(
    "asm_result, sen_result, failed_label",
    [
        (FAILED, SEN_RESULT, "ASM"),
        (ASM_RESULT, FAILED, "SEN"),
        (FAILED, FAILED, "ASM, SEN"),
    ],
)
def test_any_failed_scrape_skips_all_hearings(scrapers_return, caplog, asm_result, sen_result, failed_label):
    # Writing only one chamber would cancel the other chamber's upcoming hearings
    scrapers_return(asm_result, sen_result)

    with caplog.at_level(logging.WARNING, logger=hearings.logger.name):
        assert hearings.fetch_updates() == (set(), set())

    assert f"{failed_label} scrape failed" in caplog.text


def test_empty_scrapes_are_not_treated_as_failures(scrapers_return, caplog):
    scrapers_return((set(), set()), (set(), set()))

    with caplog.at_level(logging.WARNING, logger=hearings.logger.name):
        assert hearings.fetch_updates() == (set(), set())

    assert caplog.text == ""
