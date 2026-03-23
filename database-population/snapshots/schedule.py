"""
"""
import sources.schedule_asm_fetch as assembly
import sources.schedule_sen_fetch as senate
from config import config
from db_utils import copy_temp_table
from tqdm import tqdm

# Index into credentials.ini for globals
SNAPSHOT_SCHEMA = config("postgresql_schemas")["snapshot_schema"]
CURRENT_SESSION = config("resources")["session"]
# global vars for table names used across stages
MAIN_TABLE = "bill_schedule"
STAGE_KNOWN_TABLE = "stage_known_" + MAIN_TABLE
STAGE_KNOWN_VALID_TABLE = "stage_known_valid_" + MAIN_TABLE
STAGE_NEW_TABLE = "stage_new_" + MAIN_TABLE
STAGE_NEW_ID_TABLE = "stage_new_id_" + MAIN_TABLE
STAGE_NEW_VALID_TABLE = "stage_new_valid_" + MAIN_TABLE

def fetch_updates():
    assembly_hearings, assembly_update, assembly_changes = assembly.scrape_committee_hearing(verbose=True)
    senate_hearings, senate_update, senate_changes = senate.scrape_committee_hearing(verbose=True)

    print(f"{len(assembly_hearings)} upcoming Assembly events")
    print(f"{len(senate_hearings)} upcoming Senate events")

    # join sets before returning
    final_hearings = assembly_hearings | senate_hearings
    final_update = assembly_update | senate_update
    final_changes = assembly_changes | senate_changes

    return final_hearings, final_update, final_changes


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


def update(cur, schedule_data, schedule_changes, dev=True):
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