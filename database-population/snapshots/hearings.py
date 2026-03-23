"""
"""
from config import config
from tqdm import tqdm

# Index into credentials.ini for globals
SNAPSHOT_SCHEMA = config("postgresql_schemas")["snapshot_schema"]
CURRENT_SESSION = config("resources")["session"]
HEARINGS_TABLE = "hearings"
HEARING_BILLS_TABLE = "hearing_bills"
STAGE_HEARING_BILLS_TABLE = "stage_" + HEARING_BILLS_TABLE

def truncate_hearings(cur):
    truncate_query = "TRUNCATE TABLE {0}.{1} RESTART IDENTITY CASCADE"
    cur.execute(truncate_query.format(SNAPSHOT_SCHEMA, HEARINGS_TABLE))
    print("Truncated hearings table")
    print(cur.statusmessage)


def insert_hearings(cur, hearings_data):
    insert_query = """
        INSERT INTO {0}.{1} (chamber_id, date, name, time, location, room, notes)
        VALUES (%s, %s::DATE, %s, %s, %s, %s, %s)
    """
    for row in tqdm(hearings_data):
        cur.execute(insert_query.format(SNAPSHOT_SCHEMA, HEARINGS_TABLE), tuple(row))
    print(f"Inserted {len(hearings_data)} hearings")
    print(cur.statusmessage)


def update_hearing_committee_ids(cur):
    update_query = """
        UPDATE {0}.{1} h
        SET committee_id = c.committee_id
        FROM {0}.committee c
        WHERE h.name = c.name
        AND h.chamber_id = c.chamber_id
        AND h.committee_id IS NULL
    """
    cur.execute(update_query.format(SNAPSHOT_SCHEMA, HEARINGS_TABLE))
    print("Updated committee IDs where name match found")
    print(cur.statusmessage)

def stage_hearing_bills(cur, hearing_bills_data):
    create_query = """
        CREATE TEMPORARY TABLE {0} (
            chamber_id INT,
            event_date DATE,
            event_text TEXT,
            bill_number TEXT,
            file_order INT,
            event_time TEXT,
            event_location TEXT,
            event_room TEXT
        );
    """
    cur.execute(create_query.format(STAGE_HEARING_BILLS_TABLE))

    insert_query = """
        INSERT INTO {0} (chamber_id, event_date, event_text, bill_number, file_order, event_time, event_location, event_room)
        VALUES (%s, %s::DATE, %s, %s, %s::INT, %s, %s, %s)
    """
    for row in tqdm(hearing_bills_data):
        cur.execute(insert_query.format(STAGE_HEARING_BILLS_TABLE), tuple(row))
    print(f"Staged {len(hearing_bills_data)} hearing bill rows")


def insert_hearing_bills(cur):
    insert_query = """
        WITH resolved AS (
            SELECT
                h.hearing_id,
                b.openstates_bill_id,
                s.file_order
            FROM {0} s
            JOIN {1}.{2} h
                ON s.chamber_id = h.chamber_id
                AND s.event_date = h.date
                AND s.event_text = h.name
                AND s.event_time = h.time
                AND s.event_location = h.location
                AND s.event_room = h.room
            JOIN {1}.bill b
                ON s.bill_number = b.bill_num
                AND b.session = '{3}'
        )
        INSERT INTO {1}.{4} (hearing_id, openstates_bill_id, file_order)
        SELECT hearing_id, openstates_bill_id, file_order
        FROM resolved;
    """
    cur.execute(insert_query.format(
        STAGE_HEARING_BILLS_TABLE,
        SNAPSHOT_SCHEMA,
        HEARINGS_TABLE,
        CURRENT_SESSION,
        HEARING_BILLS_TABLE
    ))
    print("Inserted hearing bills")
    print(cur.statusmessage)


def log_dropped_hearing_bills(cur):
    # Log rows that failed to join on either hearing or bill
    log_query = """
        SELECT s.bill_number, s.event_text, s.event_date, s.chamber_id
        FROM {0} s
        WHERE NOT EXISTS (
            SELECT FROM {1}.{2} h
            WHERE s.chamber_id = h.chamber_id
            AND s.event_date = h.date
            AND s.event_text = h.name
            AND s.event_time = h.time
            AND s.event_location = h.location
            AND s.event_room = h.room
        )
        OR NOT EXISTS (
            SELECT FROM {1}.bill b
            WHERE s.bill_number = b.bill_num
            AND b.session = '{3}'
        );
    """
    cur.execute(log_query.format(
        STAGE_HEARING_BILLS_TABLE,
        SNAPSHOT_SCHEMA,
        HEARINGS_TABLE,
        CURRENT_SESSION
    ))
    dropped = cur.fetchall()
    if dropped:
        print(f"WARNING: {len(dropped)} hearing bill rows could not be matched and were dropped:")
        for row in dropped:
            print(f"  bill={row[0]}, hearing={row[1]}, date={row[2]}, chamber={row[3]}")
    else:
        print("All hearing bill rows matched successfully")


def drop_stage_hearing_bills(cur):
    cur.execute(f"DROP TABLE IF EXISTS {STAGE_HEARING_BILLS_TABLE}")
    print(cur.statusmessage)


def hearing_bills_update(cur, hearing_bills_data):
    stage_hearing_bills(cur, hearing_bills_data)
    log_dropped_hearing_bills(cur)
    insert_hearing_bills(cur)
    drop_stage_hearing_bills(cur)
    return

def update(cur, hearings_data, hearing_bills_data):
    truncate_hearings(cur) # cascades to hearing_bills automatically
    insert_hearings(cur, hearings_data)
    update_hearing_committee_ids(cur)
    hearing_bills_update(cur, hearing_bills_data)
    return