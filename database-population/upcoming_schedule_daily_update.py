"""
Connects to postgreSQL database, stages and inserts new rows to upcoming events for CA Senate and State Assembly.

After pruning events in the past from the bill_schedule table, stage new events in a temporary table. Join these rows
on their internal bill_id (and filter these rows out if a bill_id cannot be found). Insert these verified rows into
the live bill_schedule table and remove all staging tables.
"""

import dailyfile_assembly_scraper as assembly
import dailyfile_senate_scraper as senate
import pandas as pd
import db_utils
from config import config
import psycopg2
import pickle
import os
from tqdm import tqdm
from itertools import chain

LEGTRACKER_SCHEMA = config("postgresql_schemas")["legtracker_schema"]
SNAPSHOT_SCHEMA = config("postgresql_schemas")["snapshot_schema"]
CURRENT_SESSION = "20252026"

# global vars for table names used across stages
MAIN_TABLE = "bill_schedule_test"
STAGE_OLD_TABLE = "stage_old_" + MAIN_TABLE
STAGE_NEW_TABLE = "stage_new_" + MAIN_TABLE
STAGE_ID_TABLE = "stage_id_" + MAIN_TABLE


def establish_schedule(cur):
    # constraint bill_schedule_details covers edge case 1
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
            CONSTRAINT bill_schedule_info UNIQUE(openstates_bill_id, chamber_id, event_date, event_text),
            CONSTRAINT bill_schedule_details UNIQUE(openstates_bill_id, chamber_id, event_date, event_text, agenda_order, event_time, event_location, event_room)
        );
    """
    cur.execute(create_query.format(LEGTRACKER_SCHEMA, MAIN_TABLE))
    print(cur.statusmessage)
    return


def stage_old_schedule(cur):
    # Create staging table for events that are still upcoming
    temp_table_query = """
        CREATE TEMPORARY TABLE {0} AS
        SELECT * FROM {1}.{2}
        WHERE event_date >= CURRENT_DATE;
    """
    print("Staging known events")
    cur.execute(temp_table_query.format(STAGE_OLD_TABLE, LEGTRACKER_SCHEMA, MAIN_TABLE))
    print(cur.statusmessage)
    return


def stage_new_schedule(cur, schedule_data):
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

    # copy_temp_table(cur, temp_table_name)
    return


def join_filter_ids(cur):
    # Join on bill_number and filter if bill_id is not found
    temp_table_query = """
        CREATE TEMPORARY TABLE {0} AS
        SELECT nbs.chamber_id, b.openstates_bill_id, nbs.event_date, nbs.event_text, nbs.agenda_order, nbs.event_time, nbs.event_location, nbs.event_room
        FROM {1} nbs
        JOIN {2}.bill b ON nbs.bill_number = b.bill_num
        AND b.session = '{3}';
    """
    cur.execute(
        temp_table_query.format(
            STAGE_ID_TABLE, STAGE_NEW_TABLE, SNAPSHOT_SCHEMA, CURRENT_SESSION
        )
    )
    print("Joining on Openstates IDs")
    print(cur.statusmessage)
    return

def prune_bill_schedule(cur):
    # Truncate query
    truncate_query = "TRUNCATE TABLE {0}.{1}"
    # Truncate bill_schedule
    cur.execute(truncate_query.format(LEGTRACKER_SCHEMA, MAIN_TABLE))
    print("Pruning main table")
    print(cur.statusmessage)
    # copy_temp_table(cur, temp_table_name)
    return


def insert_schedule(cur):
    # Insert data into bill_schedule and update event text with newer data on conflict (edge case 2)

    insert_query = """
        INSERT INTO {0}.{1} (chamber_id, event_date, event_text, openstates_bill_id, agenda_order, event_time, event_location, event_room)
        SELECT sw.chamber_id, sw.event_date, sw.event_text, sw.openstates_bill_id, sw.agenda_order, sw.event_time, sw.event_location, sw.event_room
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
    cur.execute(insert_query.format(LEGTRACKER_SCHEMA, MAIN_TABLE, STAGE_OLD_TABLE))
    print("All known events re-inserted")
    print(cur.statusmessage)

    # # insert the events that were just scraped
    cur.execute(insert_query.format(LEGTRACKER_SCHEMA, MAIN_TABLE, STAGE_ID_TABLE))
    print("New events inserted")
    print(cur.statusmessage)

    return

def update_event_notes(cur, changed_events):  # deal with edge case 3
    # assumes we have a set of known events + the HTML note value "postponed" OR "cancelled"
    # check set length
    if len(changed_events):
        print("Preparing to change event status")
    
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

        for change in tqdm(changed_events):
            temp = update_query.format(LEGTRACKER_SCHEMA, MAIN_TABLE, *change)
            print(temp)
            # Unpack all tuple elements in order
            cur.execute(temp)
            
            # log if the row cannot be found
            if cur.statusmessage == 'UPDATE 0':
                print("Could not find rows matching these attributes:")
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
    cur.execute(drop_query.format(STAGE_ID_TABLE))
    print(cur.statusmessage)
    cur.execute(drop_query.format(STAGE_NEW_TABLE))
    print(cur.statusmessage)
    cur.execute(drop_query.format(STAGE_OLD_TABLE))
    print(cur.statusmessage)
    return


def legtracker_update(cur, schedule_data, schedule_changes, dev=True):
    # establish that the table exists
    establish_schedule(cur)
    # stage existing events to temp table
    stage_old_schedule(cur)
    # stage new events to a separate temp table
    stage_new_schedule(cur, schedule_data)
    # filter and join new events to openstates bill ID
    join_filter_ids(cur)
    # truncate past events
    prune_bill_schedule(cur)
    # insert all events
    insert_schedule(cur)
    # update known events with event_status IFF changed events are detected from scrape
    update_event_notes(cur, schedule_changes)
    # remove staging tables
    remove_staging_table(cur)

    if not dev:
        # remove pickle file in prod setting
        os.remove("schedule.pickle")
    return


def fetch_schedule_update():
    # Feature dev setting: check if schedule updates have been pickled for use
    if os.path.exists("schedule.pickle") and os.path.exists("changes.pickle"):  # If the pickle file exists, use it
        print("Loading cached schedule updates...")
        with open("schedule.pickle", mode="rb") as schedule_f:
            final_update = pickle.load(schedule_f)
        with open("changes.pickle", mode="rb") as change_f:
            final_changes = pickle.load(change_f)

    else:  # Otherwise, just fetch as normal
        print("Fetching schedule updates...")
        assembly_update, assembly_changes = assembly.scrape_dailyfile(verbose=True)
        senate_update, senate_changes = senate.scrape_dailyfile(verbose=True)

        print(f"{len(assembly_update)} upcoming Assembly events")
        print(f"{len(senate_update)} upcoming Senate events")

        # join sets before returning
        final_update = assembly_update | senate_update
        final_changes = assembly_changes | senate_changes

        # Pickle results which will be removed at the end
        with open("schedule.pickle", mode="wb") as f:
            pickle.dump(final_update, f)
        print("Updates have been pickled")
        with open("changes.pickle", mode="wb") as f:
            pickle.dump(final_changes, f)
        print("Changes have been pickled")
    return final_update, final_changes

def main():
    conn = None
    try:
        # read connection parameters
        params = config("postgresql")

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        schedule_updates, schedule_changes = fetch_schedule_update()

        if len(schedule_updates):  # Check if we have stuff before updating tables
            print("Updating bill schedule for both chambers...")
            legtracker_update(cur, schedule_updates, schedule_changes)
        else:
            print("No schedule updates; finishing")

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
