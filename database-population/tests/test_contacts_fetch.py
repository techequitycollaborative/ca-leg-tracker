import logging

from snapshots import contacts


def _raise_for(failing):
    def fake_extract(chamber):
        if chamber in failing:
            raise TypeError("bad operand type for unary ~: 'float'")
        return {}

    return fake_extract


def test_fetch_skips_failing_chamber(monkeypatch, caplog):
    monkeypatch.setattr(contacts.codex, "extract_contacts", _raise_for({"asm"}))
    with caplog.at_level(logging.WARNING, logger=contacts.logger.name):
        result = contacts.fetch_updates()

    assert result == {"upper": {}}
    assert any(
        r.levelno == logging.WARNING and "asm" in r.getMessage() for r in caplog.records
    )


def test_fetch_skips_all_chambers(monkeypatch):
    monkeypatch.setattr(contacts.codex, "extract_contacts", _raise_for({"asm", "sen"}))
    assert contacts.fetch_updates() == {}
