"""
"""
import sources.schedule_asm_fetch as assembly
import sources.schedule_sen_fetch as senate
from config import config
from tqdm import tqdm


# Index into credentials.ini for globals
SNAPSHOT_SCHEMA = config("postgresql_schemas")["snapshot_schema"]
CURRENT_SESSION = config("resources")["session"]
HEARINGS_TABLE = "hearings"
HEARING_BILLS_TABLE = "hearing_bills"
STAGE_HEARING_BILLS_TABLE = "stage_" + HEARING_BILLS_TABLE


def fetch_updates():
    assembly_hearings, assembly_bills = assembly.scrape_committee_hearing(verbose=True)
    senate_hearings, senate_bills = senate.scrape_committee_hearing(verbose=True)

    print(f"[ASM] {len(assembly_hearings)} hearings; {len(assembly_bills)} bills retrieved")
    print(f"[SEN] {len(senate_hearings)} hearings; {len(senate_bills)} bills retrieved")

    # join sets before returning
    final_hearings = assembly_hearings | senate_hearings
    final_bills = assembly_bills | senate_bills

    return final_hearings, final_bills


def truncate_hearings(cur):
    truncate_query = "TRUNCATE TABLE {0}.{1} RESTART IDENTITY CASCADE"
    cur.execute(truncate_query.format(SNAPSHOT_SCHEMA, HEARINGS_TABLE))
    print("Truncated hearings table")
    print(cur.statusmessage)


def insert_hearings(cur, hearings_data):
    insert_query = """
        INSERT INTO {0}.{1} (
            chamber_id, 
            name, 
            date, 
            time_verbatim,
            time_normalized,
            is_allday,
            location, 
            room, 
            notes
        )
        VALUES (%s, %s, %s::DATE, %s, %s::TIME, %s, %s, %s, %s)
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
        WHERE LOWER(h.name) = LOWER(c.name)
        AND h.chamber_id = c.chamber_id
        AND h.committee_id IS NULL
    """
    cur.execute(update_query.format(SNAPSHOT_SCHEMA, HEARINGS_TABLE))
    print("Updated committee IDs where name match found")
    print(cur.statusmessage)



def update_joint_hearing_chamber_id(cur):
    update_query = """
        UPDATE {0}.{1}
        SET chamber_id = CASE
            WHEN LOWER(name) LIKE '%assembly%' AND LOWER(name) LIKE '%senate%' THEN 5
            WHEN chamber_id = 1 THEN 3
            WHEN chamber_id = 2 THEN 4
            ELSE chamber_id
        END
        WHERE LOWER(name) LIKE '%joint%'
    """
    cur.execute(update_query.format(SNAPSHOT_SCHEMA, HEARINGS_TABLE))
    print("Updated joint hearing chamber IDs")
    print(cur.statusmessage)


def stage_hearing_bills(cur, hearing_bills_data):
    create_query = """
        CREATE TEMPORARY TABLE {0} (
            chamber_id INT,
            event_date DATE,
            event_text TEXT,
            bill_number TEXT,
            file_order INT,
            event_time_verbatim TEXT,
            event_location TEXT,
            event_room TEXT
        );
    """
    cur.execute(create_query.format(STAGE_HEARING_BILLS_TABLE))

    insert_query = """
        INSERT INTO {0} (chamber_id, event_date, event_text, bill_number, file_order, event_time_verbatim, event_location, event_room)
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
                AND s.event_time_verbatim = h.time_verbatim
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
            AND s.event_time_verbatim = h.time_verbatim
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


DEADLINE_LEAD_DAYS = 7
DEADLINE_TYPE = "letter"

def insert_hearing_deadlines(cur, lead_days=DEADLINE_LEAD_DAYS, deadline_type=DEADLINE_TYPE):
    insert_query = """
        INSERT INTO {0}.hearing_deadlines (hearing_id, deadline_date, deadline_type)
        SELECT
            h.hearing_id,
            h.date - INTERVAL '{1} days' AS deadline_date,
            %s AS deadline_type
        FROM {0}.hearings h
        ON CONFLICT ON CONSTRAINT unique_deadline DO NOTHING;
    """
    cur.execute(insert_query.format(SNAPSHOT_SCHEMA, lead_days), (deadline_type,))
    print(f"Inserted hearing deadlines ({lead_days} day lead, type='{deadline_type}')")
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
    update_joint_hearing_chamber_id(cur)
    hearing_bills_update(cur, hearing_bills_data)
    insert_hearing_deadlines(cur)
    return