"""DB-write tests for bills.upsert against the real database inside a transaction that is always rolled back (see conftest.py)."""

from snapshots import bills
from tests.conftest import SNAPSHOT_SCHEMA, drop_temp_tables

BILL_TEMP_TABLES = ("bill_temp", "bill_action_temp", "bill_sponsor_temp", "bill_vote_temp")


def child_counts(cur, bill_id):
    counts = {}
    for table in ("bill_action", "bill_sponsor", "bill_vote"):
        cur.execute(
            f"SELECT COUNT(*) FROM {SNAPSHOT_SCHEMA}.{table} WHERE openstates_bill_id = %s",
            (bill_id,),
        )
        counts[table] = cur.fetchone()[0]
    return counts


def test_last_update_timestamp(patched_db):
    assert bills.get_last_update_timestamp() is not None


def test_bills_upsert_insert_then_update(rollback_cur, bill_updates, marker):
    cur = rollback_cur
    bills.upsert(cur, bill_updates)

    cur.execute(
        f"SELECT title, session FROM {SNAPSHOT_SCHEMA}.bill WHERE openstates_bill_id = %s",
        (marker,),
    )
    assert cur.fetchone() == (f"{marker} title", bill_updates["bills"].at[0, "session"])
    assert child_counts(cur, marker) == {"bill_action": 1, "bill_sponsor": 1, "bill_vote": 1}

    # Second upsert exercises ON CONFLICT DO UPDATE and delete/replace of children
    drop_temp_tables(cur, *BILL_TEMP_TABLES)
    bill_updates["bills"].at[0, "title"] = f"{marker} updated"
    bills.upsert(cur, bill_updates)

    cur.execute(
        f"SELECT title FROM {SNAPSHOT_SCHEMA}.bill WHERE openstates_bill_id = %s",
        (marker,),
    )
    assert cur.fetchall() == [(f"{marker} updated",)]
    assert child_counts(cur, marker) == {"bill_action": 1, "bill_sponsor": 1, "bill_vote": 1}
