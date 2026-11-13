"""End-to-end pipeline run with fetches stubbed out, writing against the real database inside a transaction that is always rolled back (see conftest.py)."""

import logging

import pipeline
from snapshots import bills, contacts, hearings
from tests.conftest import SNAPSHOT_SCHEMA


def test_run_pipeline_end_to_end(
    patched_db,
    monkeypatch,
    caplog,
    bill_updates,
    contact_updates,
    hearing_schedule,
    bill_schedule,
    marker,
):
    monkeypatch.setattr(bills, "fetch_updates", lambda *a, **kw: bill_updates)
    monkeypatch.setattr(contacts, "fetch_updates", lambda: contact_updates)
    monkeypatch.setattr(
        hearings, "fetch_updates", lambda: (hearing_schedule, bill_schedule)
    )

    with caplog.at_level(logging.INFO):
        pipeline.run_pipeline(dev_mode=True)

    # views.refresh() and contacts.update() log errors instead of raising
    errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert errors == []

    cur = patched_db
    assert count_rows(cur, "bill WHERE openstates_bill_id = %s", marker) == 1
    assert count_rows(cur, "people_contacts WHERE staffer_contact = %s", f"{marker} Staffer") == 1

    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM {SNAPSHOT_SCHEMA}.hearing_bills hb
        JOIN {SNAPSHOT_SCHEMA}.hearings h ON h.hearing_id = hb.hearing_id
        WHERE h.name = %s AND hb.openstates_bill_id = %s
        """,
        (f"{marker} Committee", marker),
    )
    assert cur.fetchone()[0] == 1


def count_rows(cur, table_and_filter, *params):
    cur.execute(f"SELECT COUNT(*) FROM {SNAPSHOT_SCHEMA}.{table_and_filter}", params)
    return cur.fetchone()[0]
