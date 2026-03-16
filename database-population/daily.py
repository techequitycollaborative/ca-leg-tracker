"""
This script executes daily updates for both the snapshot (OpenStates) and
front-end (app) schemas.

Input: optional flag for forced update
Output: None; database update messages printed to console/log.

First, fetch each page of possible updates from OpenStates and write to a Pandas
dataframe. The first stage of manipulation is upserting a 'snapshot' of
Openstates data to the snapshot schema. Next, this snapshot schema's content is
wrangled into the front-end tables. Database manipulation is done by executing
SQL queries with formatted Python strings and psycopg2 commands.

Next, Assembly + Senate websites are scraped to retrieve upcoming hearings and
the bills that are scheduled for those hearings. To update the snapshot, we
truncate the existing tables and insert the scraped results.

Finally, the materiralized views maintained in the app schema are refreshed,
completing the DB transaction.
"""

import argparse
import bill_openstates_fetch as openstates
import committee_bill_asm_fetch as assembly
import committee_bill_sen_fetch as senate
import pandas as pd
from config import config
import psycopg2
from io import StringIO
import csv
from datetime import datetime
from db_utils import copy_temp_table
from tqdm import tqdm
from yaml import safe_load

# Index into credentials.ini for globals
SNAPSHOT_SCHEMA = config("postgresql_schemas")["snapshot_schema"]
APP_SCHEMA = config("postgresql_schemas")["app_schema"]
CURRENT_SESSION = config("resources")["session"]
LAST_UPDATED_DEFAULT = config("resources")["default_timestamp"]
REQUEST_CONFIG = safe_load(open(config("resources")["request_config"]))
BILL_COLUMNS = REQUEST_CONFIG["BILL_COLUMNS"]
BILL_ACTION_COLUMNS = REQUEST_CONFIG["BILL_ACTION_COLUMNS"]
BILL_SPONSOR_COLUMNS = REQUEST_CONFIG["BILL_SPONSOR_COLUMNS"]
BILL_VOTE_COLUMNS = REQUEST_CONFIG["BILL_VOTE_COLUMNS"]


def get_buffer(df):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep="\t", quoting=csv.QUOTE_NONE)
    buffer.seek(0)
    return buffer


def get_last_update_timestamp(cur):
    """
    Input: psycopg2 cursor
    Output: timestamp string

    Retrieves a timestamp of the most recently updated bill, or default value
    """
    query = "SELECT MAX(updated_at) FROM {0}.bill"

    cur.execute(query.format(SNAPSHOT_SCHEMA))
    last_updated = cur.fetchone()[0]

    if last_updated == "" or last_updated == None:
        last_updated = LAST_UPDATED_DEFAULT

    return last_updated


def fetch_bill_updates(updated_since=LAST_UPDATED_DEFAULT, max_page=1000, start_page=1):
    """
    Input: timestamp, max page number, start page number
    Output: dictionary from string keys to DataFrame values

    Fetch arrays of bill and bill actions/sponsors/votes since last update
    """

    df_bills = pd.DataFrame(columns=BILL_COLUMNS)
    df_bill_actions = pd.DataFrame(columns=BILL_ACTION_COLUMNS)
    df_bill_sponsors = pd.DataFrame(columns=BILL_SPONSOR_COLUMNS)
    df_bill_votes = pd.DataFrame(columns=BILL_VOTE_COLUMNS)

    current_page = start_page - 1
    num_pages = start_page

    while current_page < num_pages and current_page < max_page:
        current_page = current_page + 1
        # Imported function from fetching scripts
        data, num_pages = openstates.get_bill_data(
            page=current_page, updated_since=updated_since
        )
        print(
            "Finished fetching page "
            + str(current_page)
            + " of "
            + str(num_pages)
            + " of bill updates"
        )

        df_bills = pd.concat(
            [df_bills, pd.DataFrame(data=data["bills"], columns=BILL_COLUMNS)],
            ignore_index=True,
        )
        df_bill_actions = pd.concat(
            [
                df_bill_actions,
                pd.DataFrame(data=data["bill_actions"], columns=BILL_ACTION_COLUMNS),
            ],
            ignore_index=True,
        )
        df_bill_sponsors = pd.concat(
            [
                df_bill_sponsors,
                pd.DataFrame(data=data["bill_sponsors"], columns=BILL_SPONSOR_COLUMNS),
            ],
            ignore_index=True,
        )
        df_bill_votes = pd.concat(
            [
                df_bill_votes,
                pd.DataFrame(data=data["bill_votes"], columns=BILL_VOTE_COLUMNS),
            ],
            ignore_index=True,
        )

    return {
        "bills": df_bills,
        "bill_actions": df_bill_actions,
        "bill_sponsors": df_bill_sponsors,
        "bill_votes": df_bill_votes,
    }


def openstates_upsert_bills(cur, bills):
    """
    Input: psycopg2 cursor, array of bill data in Openstates structure
    Output: None (creates temporary CSV and executes SQL queries)

    After writing all new bills to a temp CSV file, use temp SQL table to upsert live bills table
    in the SNAPSHOT_SCHEMA
    """

    # Create a temporary bill table by copying SNAPSHOT_SCHEMA.bill columns
    # WHERE false clause is shortcut to creating empty temporary table
    temp_table_name = "bill_temp"
    temp_table_query = """
        CREATE TEMPORARY TABLE {0} AS
        SELECT *
        FROM {1}.bill
        WHERE false
    """
    cur.execute(temp_table_query.format(temp_table_name, SNAPSHOT_SCHEMA))

    # Create a string buffer
    buffer = StringIO()

    # Given list of bills fetched from OpenStates, write to buffer
    bills.to_csv(
        buffer,
        index=False,
        header=False,
        sep="\t",
        quoting=csv.QUOTE_NONE,
        escapechar="\\",
    )
    buffer.seek(0)

    # Bulk insert from buffer to temp table
    cur.copy_from(file=buffer, table=temp_table_name, sep="\t", columns=BILL_COLUMNS)

    # Insert new rows to bill table from temp and update existing rows with temp values
    update_bills_query = """
        INSERT INTO {0}.bill
        SELECT *
        FROM {1}
        ON CONFLICT (openstates_bill_id) DO UPDATE SET
            session=EXCLUDED.session,
            chamber=EXCLUDED.chamber,
            bill_num=EXCLUDED.bill_num,
            title=EXCLUDED.title,
            created_at=EXCLUDED.created_at,
            updated_at=EXCLUDED.updated_at,
            first_action_date=EXCLUDED.first_action_date,
            last_action_date=EXCLUDED.last_action_date,
            abstract=EXCLUDED.abstract
    """
    cur.execute(update_bills_query.format(SNAPSHOT_SCHEMA, temp_table_name))
    print("Snapshot upsert main bill table")
    print(cur.statusmessage)


def openstates_update_bill_data(
    cur, bill_list=[], bill_actions=[], bill_sponsors=[], bill_votes=[]
):
    """
    Input: psycopg2 cursor, list of bill IDs, bill actions, bill sponsors, bill votes
    Output: None (creates temporary CSV and executes SQL queries)

    Deletes existing and inserts all actions, sponsors, and votes in Openstates structure for the specified
    list of bill IDs. Creates temporary tables which are filled from CSV (via buffer), then updates live tables
    with temporary tables.
    """
    # Table names
    bill_action = "bill_action"
    bill_sponsor = "bill_sponsor"
    bill_vote = "bill_vote"

    # Create temporary tables
    temp_table_query = """
        CREATE TEMPORARY TABLE {0}_temp AS
        SELECT *
        FROM {1}.{0}
        WHERE false
    """
    cur.execute(temp_table_query.format(bill_action, SNAPSHOT_SCHEMA))
    cur.execute(temp_table_query.format(bill_sponsor, SNAPSHOT_SCHEMA))
    cur.execute(temp_table_query.format(bill_vote, SNAPSHOT_SCHEMA))

    # Load new data to temporary tables
    cur.copy_from(
        file=get_buffer(bill_actions),
        table=bill_action + "_temp",
        sep="\t",
        columns=BILL_ACTION_COLUMNS,
    )
    cur.copy_from(
        file=get_buffer(bill_sponsors),
        table=bill_sponsor + "_temp",
        sep="\t",
        columns=BILL_SPONSOR_COLUMNS,
    )
    cur.copy_from(
        file=get_buffer(bill_votes),
        table=bill_vote + "_temp",
        sep="\t",
        columns=BILL_VOTE_COLUMNS,
    )

    # Delete old data from live tables
    bill_ids_string = "'" + "','".join(bill_list) + "'"
    delete_query = """
        DELETE FROM {0}.{1}
        WHERE openstates_bill_id IN ({2})
    """
    print("Delete old actions, sponsor, vote snapshots")
    cur.execute(delete_query.format(SNAPSHOT_SCHEMA, bill_action, bill_ids_string))
    print(cur.statusmessage)
    cur.execute(delete_query.format(SNAPSHOT_SCHEMA, bill_sponsor, bill_ids_string))
    print(cur.statusmessage)
    cur.execute(delete_query.format(SNAPSHOT_SCHEMA, bill_vote, bill_ids_string))
    print(cur.statusmessage)

    # Copy new data to live tables
    update_data_query = """
        INSERT INTO {0}.{1}
        SELECT *
        FROM {2}
    """
    print("Update actions, sponsor, vote snapshots with new data")
    cur.execute(
        update_data_query.format(SNAPSHOT_SCHEMA, bill_action, bill_action + "_temp")
    )
    print(cur.statusmessage)
    cur.execute(
        update_data_query.format(SNAPSHOT_SCHEMA, bill_sponsor, bill_sponsor + "_temp")
    )
    print(cur.statusmessage)
    cur.execute(
        update_data_query.format(SNAPSHOT_SCHEMA, bill_vote, bill_vote + "_temp")
    )
    print(cur.statusmessage)


def fetch_schedule_update():
    assembly_update, assembly_changes = assembly.scrape_committee_hearing(verbose=True)
    senate_update, senate_changes = senate.scrape_committee_hearing(verbose=True)

    print(f"{len(assembly_update)} upcoming Assembly events")
    print(f"{len(senate_update)} upcoming Senate events")

    # join sets before returning
    final_update = assembly_update | senate_update
    final_changes = assembly_changes | senate_changes

    return final_update, final_changes


def refresh_snapshot_views(cur):
    bills_query = """
        REFRESH MATERIALIZED VIEW CONCURRENTLY {0}.bills_mv
    """
    bill_history_query = """
        REFRESH MATERIALIZED VIEW CONCURRENTLY {0}.bill_history_mv
    """
    # TODO: include legislators, which should also be a materialized view

    print("Refreshing materialized view - bills")
    cur.execute(bills_query.format(APP_SCHEMA))
    print(cur.statusmessage)

    print("Refreshing materialized view - bill history")
    cur.execute(bill_history_query.format(APP_SCHEMA))
    print(cur.statusmessage)
    return


# global vars for table names used across stages
MAIN_TABLE = "bill_schedule"
STAGE_KNOWN_TABLE = "stage_known_" + MAIN_TABLE
STAGE_KNOWN_VALID_TABLE = "stage_known_valid_" + MAIN_TABLE
STAGE_NEW_TABLE = "stage_new_" + MAIN_TABLE
STAGE_NEW_ID_TABLE = "stage_new_id_" + MAIN_TABLE
STAGE_NEW_VALID_TABLE = "stage_new_valid_" + MAIN_TABLE


def establish_schedule(cur):
    # constraint bill_schedule_aux helps with edge case 1
    create_query = """
        CREATE TABLE IF NOT EXISTS {0}.{1} 
        (
            bill_schedule_id SERIAL PRIMARY KEY,
            openstates_bill_id TEXT,
            chamber_id INT,
            event_date DATE,
            event_text TEXT,
            agenda_order INT,
            event_time TEXT,
            event_location TEXT,
            event_room TEXT,
            revised BOOLEAN DEFAULT FALSE,
            event_status TEXT DEFAULT 'active',
            CONSTRAINT bill_schedule_core UNIQUE(openstates_bill_id, chamber_id, event_date, event_text),
            CONSTRAINT bill_schedule_aux UNIQUE(openstates_bill_id, chamber_id, event_date, event_text, agenda_order, event_time, event_location, event_room)
        );
    """
    cur.execute(create_query.format(SNAPSHOT_SCHEMA, MAIN_TABLE))
    print(cur.statusmessage)
    return


def stage_known_schedule(cur, dev):
    # Create staging table for events that are still upcoming
    temp_table_query = """
        CREATE TEMPORARY TABLE {0} AS
        SELECT * FROM {1}.{2}
        WHERE event_date >= CURRENT_DATE;
    """
    print("Copying known events")
    cur.execute(temp_table_query.format(STAGE_KNOWN_TABLE, SNAPSHOT_SCHEMA, MAIN_TABLE))
    print(cur.statusmessage)

    copy_temp_table(cur, dev, STAGE_KNOWN_TABLE)
    return


def stage_new_schedule(cur, schedule_data, dev):
    # Create staging table
    temp_table_query = """
        CREATE TEMPORARY TABLE {0} (
        chamber_id INT,
        bill_number TEXT,
        event_date DATE,
        event_text TEXT,
        agenda_order INT,
        event_time TEXT,
        event_location TEXT,
        event_room TEXT,
        revised BOOLEAN DEFAULT FALSE,
        event_status TEXT DEFAULT 'active'
        );
    """
    cur.execute(temp_table_query.format(STAGE_NEW_TABLE))
    print("Staging new events")

    # Insert data into staging table
    insert_query = """
        INSERT INTO {0} (chamber_id, event_date, event_text, bill_number, agenda_order, event_time, event_location, event_room)
        VALUES (%s, %s::DATE, %s, %s, %s::INT, %s, %s, %s)
    """
    # Execute the insert query for each row in the schedule_data
    for row in tqdm(schedule_data):
        cur.execute(insert_query.format(STAGE_NEW_TABLE), tuple(row))

    copy_temp_table(cur, dev, STAGE_NEW_TABLE)
    return


def join_filter_ids(cur, dev):
    # Join on bill_number and filter if bill_id is not found
    temp_table_query = """
        CREATE TEMPORARY TABLE {0} AS
        SELECT b.openstates_bill_id, a.*
        FROM {1} a
        JOIN {2}.bill b ON a.bill_number = b.bill_num
        AND b.session = '{3}';
    """
    cur.execute(
        temp_table_query.format(
            STAGE_NEW_ID_TABLE, STAGE_NEW_TABLE, SNAPSHOT_SCHEMA, CURRENT_SESSION
        )
    )
    print("Joining on Openstates IDs")
    print(cur.statusmessage)

    copy_temp_table(cur, dev, STAGE_NEW_ID_TABLE)
    return


def update_known_events(cur, dev):
    print(
        "Preparing to mark events as 'moved' if they don't exist in the current scraper pull"
    )

    # Left outer join on known events with the new event batch
    known_valid_query = """
        CREATE TEMPORARY TABLE {0} AS
        SELECT b.bill_number, a.*
        FROM {1} a
        LEFT JOIN {2} b
        ON 
            a.openstates_bill_id = b.openstates_bill_id AND
            a.chamber_id = b.chamber_id AND
            a.event_date = b.event_date AND
            a.event_text = b.event_text AND
            a.agenda_order = b.agenda_order AND
            a.event_time = b.event_time AND
            a.event_location = b.event_location AND
            a.event_room = b.event_room
    """

    # Leverage the blank bill_number column for edge case 5:
    # when the new event batch does not include overlap with known events,
    # set the event_status to the implied 'moved'
    # still leaving logic for event 'revisions' (edge case 2)
    update_query = """
        UPDATE {0} a
        SET event_status='moved'
        WHERE 
            bill_number IS NULL AND
            NOT EXISTS (
                SELECT
                FROM {1} b
                WHERE
                    a.openstates_bill_id = b.openstates_bill_id AND
                    a.chamber_id = b.chamber_id AND
                    a.event_date = b.event_date AND
                    a.event_text = b.event_text
            );
    """

    cur.execute(
        known_valid_query.format(
            STAGE_KNOWN_VALID_TABLE, STAGE_KNOWN_TABLE, STAGE_NEW_ID_TABLE
        )
    )
    print("Validate known events by matching them to the current update")
    print(cur.statusmessage)

    cur.execute(update_query.format(STAGE_KNOWN_VALID_TABLE, STAGE_NEW_ID_TABLE))
    print(
        "If bill number can't be matched to current update, set event status to 'moved'"
    )
    print(cur.statusmessage)

    copy_temp_table(cur, dev, STAGE_KNOWN_VALID_TABLE)
    return


def prune_bill_schedule(cur, dev):
    # Truncate query
    truncate_query = "TRUNCATE TABLE {0}.{1}"
    # Truncate bill_schedule
    cur.execute(truncate_query.format(SNAPSHOT_SCHEMA, MAIN_TABLE))
    print("Pruning main table")
    print(cur.statusmessage)

    # Pruning query - edge case 1
    prune_query = """
        CREATE TEMPORARY TABLE {0} AS
        SELECT openstates_bill_id, chamber_id, event_date, event_text, agenda_order, event_time, event_location, event_room, revised, event_status
        FROM {1} sn
        WHERE NOT EXISTS (
            SELECT
            FROM {2}
            WHERE 
                openstates_bill_id = sn.openstates_bill_id AND
                chamber_id = sn.chamber_id AND
                event_date = sn.event_date AND
                event_text = sn.event_text AND
                agenda_order = sn.agenda_order AND
                event_time = sn.event_time AND
                event_location = sn.event_location AND
                event_room = sn.event_room
        );
    """

    cur.execute(
        prune_query.format(
            STAGE_NEW_VALID_TABLE, STAGE_NEW_ID_TABLE, STAGE_KNOWN_VALID_TABLE
        )
    )
    print(cur.statusmessage)
    print("Pruned duplicate events from the set of new events")
    copy_temp_table(cur, dev, STAGE_NEW_VALID_TABLE)
    return


def insert_schedule(cur):
    # Insert data into bill_schedule and update event text with newer data on conflict (edge case 2)

    insert_query = """
        INSERT INTO {0}.{1} (chamber_id, event_date, event_text, openstates_bill_id, agenda_order, event_time, event_location, event_room, revised, event_status)
        SELECT sw.chamber_id, sw.event_date, sw.event_text, sw.openstates_bill_id, sw.agenda_order, sw.event_time, sw.event_location, sw.event_room, sw.revised, sw.event_status
        FROM {2} sw
        ON CONFLICT (openstates_bill_id, chamber_id, event_date, event_text)
        DO UPDATE SET
            agenda_order = EXCLUDED.agenda_order,
            event_time = EXCLUDED.event_time,
            event_location = EXCLUDED.event_location,
            event_room = EXCLUDED.event_room,
            revised = TRUE,
            event_status = EXCLUDED.event_status;
    """
    # insert all valid events
    cur.execute(
        insert_query.format(SNAPSHOT_SCHEMA, MAIN_TABLE, STAGE_KNOWN_VALID_TABLE)
    )
    print("All known events re-inserted")
    print(cur.statusmessage)

    # insert the events that were just scraped
    cur.execute(insert_query.format(SNAPSHOT_SCHEMA, MAIN_TABLE, STAGE_NEW_VALID_TABLE))
    print("New events inserted")
    print(cur.statusmessage)

    return


def update_event_notes(cur, changed_events):  # deal with edge case 3
    # assumes we have a set of known events + the HTML note value "postponed" OR "cancelled"
    # check set length
    if len(changed_events):
        print("Preparing to update event postponement or cancellation")

        update_query = """
            UPDATE {0}.{1}
            SET event_status='{8}'
            WHERE chamber_id={2} AND
            event_date='{3}' AND
            event_text='{4}' AND
            event_time='{5}' AND
            event_location='{6}' AND
            event_room='{7}';
        """

        for change in changed_events:
            temp = update_query.format(SNAPSHOT_SCHEMA, MAIN_TABLE, *change)
            print(temp)
            # Unpack all tuple elements in order
            cur.execute(temp)

            # log if the row cannot be found
            if cur.statusmessage == "UPDATE 0":
                print("Upcoming schedule changes do not affect tracked events")
            else:
                print(cur.statusmessage)
    else:
        print("All event statuses are up-to-date")
    return


def remove_staging_table(cur):
    # Remove staging tables
    drop_query = """
        DROP TABLE IF EXISTS {};
    """
    cur.execute(drop_query.format(STAGE_NEW_VALID_TABLE))
    print(cur.statusmessage)
    cur.execute(drop_query.format(STAGE_KNOWN_VALID_TABLE))
    print(cur.statusmessage)
    cur.execute(drop_query.format(STAGE_NEW_ID_TABLE))
    print(cur.statusmessage)
    cur.execute(drop_query.format(STAGE_NEW_TABLE))
    print(cur.statusmessage)
    cur.execute(drop_query.format(STAGE_KNOWN_TABLE))
    print(cur.statusmessage)
    return


def bill_schedule_update(cur, schedule_data, schedule_changes, dev=True):
    # establish that the table exists
    establish_schedule(cur)

    # stage existing events to temp table STAGE_KNOWN
    stage_known_schedule(cur, dev)

    # stage new events to a separate temp table STAGE_NEW
    stage_new_schedule(cur, schedule_data, dev)

    # filter and join new events to openstates bill ID, STAGE_NEW_ID
    join_filter_ids(cur, dev)

    # update known events STAGE_KNOWN with STAGE_NEW as ground truth >> STAGE_KNOWN_VALID
    update_known_events(cur, dev)

    # truncate main table, prune duplicates between STAGE_ID and STAGE_KNOWN_VALID as ground truth >> STAGE_NEW_VALID
    prune_bill_schedule(cur, dev)

    # insert STAGE_KNOWN_VALID and STAGE_NEW_VALID
    insert_schedule(cur)

    # update known events with event_status IFF changed events are detected from scrape
    update_event_notes(cur, schedule_changes)

    # remove staging tables
    remove_staging_table(cur)

    return


def main():
    # argument parser detects optional flags
    parser = argparse.ArgumentParser(
        description="Take new snapshot of OpenStates data and wrangle into legislation tracker."
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="Force update on front-end schema without date filtering.",
    )
    args = parser.parse_args()

    conn = None
    try:
        # read connection parameters
        params = config("postgresql")

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # fetch bills from openstates updated since last timestamp
        last_update = get_last_update_timestamp(cur)
        print("Last update timestamp: " + last_update)
        print(
            "Current timestamp: "
            + datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
            + " -- fetching updates from Openstates"
        )
        bill_updates = fetch_bill_updates(last_update)

        print("Summary of bills being updated:")

        # Empty OpenStates response case
        if len(bill_updates["bills"].index) == 0:
            print("Empty response from Openstates API.")

            # If flag is activated, re-sync DB schemas regardless of update
            if args.force_update:
                # Upserts new bills and new bill content into Openstates/snapshot schema
                openstates_upsert_bills(cur, bill_updates["bills"])

                # Updates bill actions, sponsors, votes from Openstates response
                openstates_update_bill_data(
                    cur=cur,
                    bill_list=bill_updates["bills"]["openstates_bill_id"],
                    bill_actions=bill_updates["bill_actions"],
                    bill_sponsors=bill_updates["bill_sponsors"],
                    bill_votes=bill_updates["bill_votes"],
                )
                print("Snapshot forced to update")
            else:
                print("Skipping forced refresh -- no bills to update; finishing.")
        else:

            # Logs new bills to console
            print(bill_updates["bills"])
            print("Openstates response received. Updating Openstates snapshot...")

            # Upserts new bills and new bill content into Openstates/snapshot schema
            openstates_upsert_bills(cur, bill_updates["bills"])

            # Updates bill actions, sponsors, votes from Openstates response
            openstates_update_bill_data(
                cur=cur,
                bill_list=bill_updates["bills"]["openstates_bill_id"],
                bill_actions=bill_updates["bill_actions"],
                bill_sponsors=bill_updates["bill_sponsors"],
                bill_votes=bill_updates["bill_votes"],
            )
            
        # Update hearings and bill schedules
        schedule_updates, schedule_changes = fetch_schedule_update()

        if len(schedule_updates):  # Check if we have stuff before updating tables
            print("Updating bill schedule for both chambers...")
            bill_schedule_update(cur, schedule_updates, schedule_changes)
        else:
            print("No schedule updates; finishing")

        # Always refresh materialized views
        refresh_snapshot_views(cur=cur)
        print("Snapshot updated")
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Failed to update records", error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed")

    print("Update finished")


if __name__ == "__main__":
    main()
