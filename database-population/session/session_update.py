"""
Orchestrates the annual session update pipeline:
- Legislator, party, district, and role information from OpenStates
- Staffer contact information from Capitol Codex

Run once per legislative session via session/main.py.
"""

import psycopg2
from datetime import datetime
from config import config
from session.snapshots.people import (
    get_last_update_timestamp,
    fetch_legislator_updates,
    upsert_people,
    update_people_data,
)
from session.snapshots.contacts import fetch_codex_updates, codex_upsert_contacts



def run_session_update(force_update=False):
    conn = None
    try:
        params = config("postgres")
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        last_update = get_last_update_timestamp(cur)
        print("Last update timestamp: " + str(last_update))
        print(
            "Current timestamp: "
            + datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
            + " -- fetching updates from OpenStates"
        )

        legislator_updates = fetch_legislator_updates(last_update)
        contact_updates = fetch_codex_updates()
        print("Summary of legislators being updated:")

        if len(legislator_updates["people"].index) == 0:
            print("Empty legislator response from OpenStates API.")

            if force_update:
                upsert_people(cur, legislator_updates["people"])
                update_people_data(
                    cur=cur,
                    people_list=legislator_updates["people"]["openstates_people_id"],
                    people_role_data=legislator_updates["people_roles"],
                    people_office_data=legislator_updates["people_offices"],
                    people_name_data=legislator_updates["people_names"],
                    people_source_data=legislator_updates["people_sources"],
                )
                print("Snapshot forced to update")
            else:
                print("Skipping forced refresh -- no legislators to update; finishing.")
        else:
            for k in legislator_updates:
                print(k)
                print(legislator_updates[k])
                print()
            print("OpenStates response received. Updating snapshot...")

            upsert_people(cur, legislator_updates["people"])
            update_people_data(
                cur=cur,
                people_list=legislator_updates["people"]["openstates_people_id"],
                people_role_data=legislator_updates["people_roles"],
                people_office_data=legislator_updates["people_offices"],
                people_name_data=legislator_updates["people_names"],
                people_source_data=legislator_updates["people_sources"],
            )

            for chamber, contact_data in contact_updates.items():
                codex_upsert_contacts(cur=cur, contact_data=contact_data, chamber=chamber)

            print("Legislator snapshot updated")

        conn.commit()

    except psycopg2.Error as e:
        print(f"[SESSION] Database error: {e.pgerror}")
    except Exception as e:
        print(f"[SESSION] Operation failed: {str(e)}")
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed")

    print("Session update finished")