import datetime as dt

from snapshots import bills, hearings
from tests.conftest import SNAPSHOT_SCHEMA, drop_temp_tables


def fetch_hearing(cur, name):
    cur.execute(
        f"SELECT hearing_id, canceled_at FROM {SNAPSHOT_SCHEMA}.hearings WHERE name = %s",
        (name,),
    )
    rows = cur.fetchall()
    assert len(rows) == 1
    return rows[0]


def test_hearings_upsert(
    rollback_cur, bill_updates, hearing_schedule, bill_schedule, hearing_date, marker
):
    cur = rollback_cur
    # hearing_bills joins on bill, so the fixture bill must exist first
    bills.upsert_bill_data(cur, bill_updates["bills"])

    hearings.upsert(cur, hearing_schedule, bill_schedule)

    hearing_id, canceled_at = fetch_hearing(cur, f"{marker} Committee")
    assert canceled_at is None

    cur.execute(
        f"""
        SELECT openstates_bill_id, file_order
        FROM {SNAPSHOT_SCHEMA}.hearing_bills
        WHERE hearing_id = %s
        """,
        (hearing_id,),
    )
    assert cur.fetchall() == [(marker, 1)]

    cur.execute(
        f"""
        SELECT deadline_date, deadline_type
        FROM {SNAPSHOT_SCHEMA}.hearing_deadlines
        WHERE hearing_id = %s
        """,
        (hearing_id,),
    )
    expected_deadline = hearing_date - dt.timedelta(days=hearings.DEADLINE_LEAD_DAYS)
    assert cur.fetchall() == [(expected_deadline, hearings.DEADLINE_TYPE)]


def test_hearings_cancel_missing(
    rollback_cur, bill_updates, hearing_schedule, bill_schedule, marker
):
    cur = rollback_cur
    bills.upsert_bill_data(cur, bill_updates["bills"])
    hearings.upsert(cur, hearing_schedule, bill_schedule)

    # Rerun with the fixture hearing absent from the feed
    drop_temp_tables(
        cur, hearings.INCOMING_HEARINGS_TABLE, hearings.INCOMING_HEARING_BILLS_TABLE
    )
    hearings.upsert(cur, set(), set())

    _, canceled_at = fetch_hearing(cur, f"{marker} Committee")
    assert canceled_at is not None
