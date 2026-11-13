"""DB-write test for contacts.update against the real database inside a transaction that is always rolled back (see conftest.py)."""

from snapshots import contacts
from tests.conftest import SNAPSHOT_SCHEMA


def test_contacts_update(rollback_cur, contact_updates, lower_district, marker):
    cur = rollback_cur
    people_id, _ = lower_district

    contacts.update(cur, contact_updates["lower"], "lower")

    cur.execute(
        f"""
        SELECT staffer_contact, generated_email, issue_area, staffer_type
        FROM {SNAPSHOT_SCHEMA}.people_contacts
        WHERE openstates_people_id = %s
        """,
        (people_id,),
    )
    # Flush + insert leaves only the staged contacts for this district
    assert cur.fetchall() == [
        (f"{marker} Staffer", f"{marker}@asm.ca.gov", "Testing", "staff")
    ]
