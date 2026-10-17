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
    cur.execute(
        f"SELECT COUNT(*) FROM {SNAPSHOT_SCHEMA}.bill WHERE openstates_bill_id = %s",
        (marker,),
    )
    assert cur.fetchone()[0] == 1
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
    cur.execute(
        f"SELECT COUNT(*) FROM {SNAPSHOT_SCHEMA}.people_contacts WHERE staffer_contact = %s",
        (f"{marker} Staffer",),
    )
    assert cur.fetchone()[0] == 1
