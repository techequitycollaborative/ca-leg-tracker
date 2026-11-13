"""Unit tests for combining Assembly and Senate hearing scrapes (no browser or DB)."""

import logging

import pytest

from snapshots import hearings

ASM_RESULT = ({("asm-hearing",)}, {("asm-bill",)})
SEN_RESULT = ({("sen-hearing",)}, {("sen-bill",)})


@pytest.fixture
def scrapers(monkeypatch):
    def install(asm_result, sen_result):
        monkeypatch.setattr(
            hearings.assembly, "scrape_committee_hearing", lambda verbose: asm_result
        )
        monkeypatch.setattr(
            hearings.senate, "scrape_committee_hearing", lambda verbose: sen_result
        )

    return install


def test_combines_both_chambers(scrapers):
    scrapers(ASM_RESULT, SEN_RESULT)

    assert hearings.fetch_updates() == (
        {("asm-hearing",), ("sen-hearing",)},
        {("asm-bill",), ("sen-bill",)},
    )


@pytest.mark.parametrize(
    "asm_result, sen_result, failed",
    [
        (None, SEN_RESULT, "ASM"),
        (ASM_RESULT, None, "SEN"),
        (None, None, "ASM, SEN"),
    ],
)
def test_failed_scrape_skips_all_hearings(scrapers, caplog, asm_result, sen_result, failed):
    # Writing only one chamber would cancel the other chamber's upcoming hearings
    scrapers(asm_result, sen_result)

    with caplog.at_level(logging.WARNING, logger=hearings.logger.name):
        assert hearings.fetch_updates() == (set(), set())

    assert any(
        r.levelno == logging.WARNING and f"{failed} scrape failed" in r.getMessage()
        for r in caplog.records
    )


def test_empty_scrapes_are_not_failures(scrapers, caplog):
    scrapers((set(), set()), (set(), set()))

    with caplog.at_level(logging.WARNING, logger=hearings.logger.name):
        assert hearings.fetch_updates() == (set(), set())

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
