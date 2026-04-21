"""
Upserts source hearing-related data into the snapshot schema.

1. Stage all incoming hearings in a temporary table
2. Upserts hearings based on diff between DB and staged incoming hearings 
3. Soft-delete hearings that are present in the DB but absent from the incoming
   data so that child records (hearing_bills, hearing_deadlines) can be preserved
   for audit purposes. Past hearings are not touched. Also logs a per-hearing
   reason derived from the notes field for diagnostics.
4. Derive committee_id values from hearing name field.
5. Derive hearing chamber_id values from hearing name field.
6. Stage incoming hearing-bill associations in a temporary table
7. Upsert hearing_bills based on diff between DB and staged incoming associations
8. Hard-delete hearing_bills rows if not included in the incoming associations
9. Log dropped bills that could not be matched for diagnostics.
10. Sync deadlines, recalculating stale ones for rescheduled hearings
"""
import db
import sources.schedule_asm_fetch as assembly
import sources.schedule_sen_fetch as senate
from config import config
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Index into credentials.ini for globals
SNAPSHOT_SCHEMA = config("postgresql_schemas")["snapshot_schema"]
CURRENT_SESSION = config("resources")["session"]
HEARINGS_TABLE = "hearings"
HEARING_BILLS_TABLE = "hearing_bills"
INCOMING_HEARINGS_TABLE = "incoming_" + HEARINGS_TABLE
INCOMING_HEARING_BILLS_TABLE = "incoming_" + HEARING_BILLS_TABLE

    

def fetch_updates():
    assembly_hearings, assembly_bills = assembly.scrape_committee_hearing(verbose=True)
    senate_hearings, senate_bills = senate.scrape_committee_hearing(verbose=True)

    logger.info(
        f"[ASM] {len(assembly_hearings)} hearings; {len(assembly_bills)} bills retrieved"
    )
    logger.info(f"[SEN] {len(senate_hearings)} hearings; {len(senate_bills)} bills retrieved")

    # join sets before returning
    final_hearings = assembly_hearings | senate_hearings
    final_bills = assembly_bills | senate_bills

    return final_hearings, final_bills


def stage_incoming_hearings(cur, hearings_data):
    

    create_query = """
        CREATE TEMPORARY TABLE {stage} (
            chamber_id      INT,
            name            TEXT,
            date            DATE,
            time_verbatim   TEXT,
            time_normalized TIME,
            is_allday       BOOLEAN,
            location        TEXT,
            room            TEXT,
            notes           TEXT
        )
    """.format(stage=INCOMING_HEARINGS_TABLE)
    cur.execute(create_query)

    insert_query = """
        INSERT INTO {stage} (
            chamber_id, name, date, time_verbatim, time_normalized, is_allday,
            location, room, notes
        )
        VALUES (%s, %s, %s::DATE, %s, %s::TIME, %s, %s, %s, %s)
    """.format(stage=INCOMING_HEARINGS_TABLE)
    
    cur.executemany(insert_query, hearings_data)

    logger.info(f"Staged incoming hearings: {cur.rowcount} rows affected")
    return


def upsert_hearings(cur):
    upsert_query = """
        INSERT INTO {schema}.{hearings} (
            chamber_id, name, date, time_verbatim, time_normalized, is_allday, 
            location, room, notes
        )
        SELECT
            CASE
                WHEN LOWER(name) LIKE '%joint legislative audit%'
                OR (LOWER(name) LIKE '%assembly%' AND LOWER(name) LIKE '%senate%') THEN 5
                WHEN LOWER(name) LIKE '%joint%' AND chamber_id = 1 THEN 3
                WHEN LOWER(name) LIKE '%joint%' AND chamber_id = 2 THEN 4
                ELSE chamber_id
            END AS chamber_id, 
            name, date, time_verbatim, time_normalized, is_allday, location, 
            room, notes
        FROM {stage}
        ON CONFLICT (chamber_id, name, date) DO UPDATE SET
            time_verbatim   = EXCLUDED.time_verbatim,
            time_normalized = EXCLUDED.time_normalized,
            is_allday       = EXCLUDED.is_allday,
            location        = EXCLUDED.location,
            room            = EXCLUDED.room,
            notes           = EXCLUDED.notes,
        canceled_at         = CASE
            WHEN (
                EXCLUDED.time_verbatim   IS DISTINCT FROM {schema}.{hearings}.time_verbatim OR
                EXCLUDED.time_normalized IS DISTINCT FROM {schema}.{hearings}.time_normalized OR
                EXCLUDED.is_allday       IS DISTINCT FROM {schema}.{hearings}.is_allday OR
                EXCLUDED.location        IS DISTINCT FROM {schema}.{hearings}.location OR
                EXCLUDED.room            IS DISTINCT FROM {schema}.{hearings}.room OR
                EXCLUDED.notes           IS DISTINCT FROM {schema}.{hearings}.notes
            ) THEN NULL
            ELSE {schema}.{hearings}.canceled_at
        END
    """.format(
            schema=SNAPSHOT_SCHEMA,
            hearings=HEARINGS_TABLE,
            stage=INCOMING_HEARINGS_TABLE
        )
    
    cur.execute(upsert_query)
    logger.info(f"Upserted hearings: {cur.rowcount} affected")
    return


# Note patterns expected from source data. Used for logging only — control
# flow relies on absence-from-feed, not string matching.
_NOTE_CANCELED = "hearing canceled"
_NOTE_MOVED = "hearing moved to"
_NOTE_ROOM = "note room change"
_NOTE_TIME = "note time change"

def _classify_cancellation_reason(notes: str | None) -> str:
    if not notes:
        return "no note"
    n = notes.strip().lower()
    if _NOTE_CANCELED in n:
        return "canceled per note"
    if _NOTE_MOVED in n:
        return f"moved per note: {notes.strip()}"
    if _NOTE_ROOM in n:
        return f"room change per note: {notes.strip()}"
    if _NOTE_TIME in n:
        return f"time change per note: {notes.strip()}"
    return f"unrecognized note: {notes.strip()}"


def cancel_missing_hearings(cur):
    fetch_query = """
        SELECT h.hearing_id, h.name, h.date, h.chamber_id, s.notes
        FROM {schema}.{hearings} h
        LEFT JOIN {stage} s
            ON s.chamber_id = h.chamber_id
            AND s.name = h.name
        WHERE h.canceled_at IS NULL
        AND h.date > CURRENT_DATE
        AND NOT EXISTS (
            SELECT 1 FROM {stage} incoming
            WHERE incoming.chamber_id = h.chamber_id
            AND incoming.name = h.name
            AND incoming.date = h.date
        )
    """.format(
            schema=SNAPSHOT_SCHEMA, 
            hearings=HEARINGS_TABLE, 
            stage=INCOMING_HEARINGS_TABLE
            )
    
    cur.execute(fetch_query)
    
    to_cancel = cur.fetchall()

    if not to_cancel:
        logger.info("No hearings to cancel")
        return
    
    # Log each cancellation with a reason before writing
    for hearing_id, name, date, chamber_id, notes in to_cancel:
        reason = _classify_cancellation_reason(notes)
        logger.info(
            (
                f"    Canceling hearing_id={hearing_id} "
                f"name='{name}' "
                f"date={date} "
                f"chamber={chamber_id} | "
                f"reason: {reason}"
            )
        )
    
    # Add value to canceled_at
    cancel_query = """
        UPDATE {schema}.{hearings} h
        SET canceled_at = CURRENT_TIMESTAMP
        WHERE h.canceled_at IS NULL
        AND h.date >= CURRENT_DATE
        AND NOT EXISTS (
            SELECT 1 FROM {stage} s
            WHERE s.chamber_id = h.chamber_id
            AND s.name = h.name
            AND s.date = h.date
        )
    """.format(
            schema=SNAPSHOT_SCHEMA, 
            hearings=HEARINGS_TABLE, 
            stage=INCOMING_HEARINGS_TABLE
        )
    
    cur.execute(cancel_query)
    logger.info(f"Canceled hearings missing from incoming data: {cur.rowcount} affected")
    return


def update_hearing_committee_ids(cur):
    update_query = """
        UPDATE {schema}.{hearings} h
        SET committee_id = c.committee_id
        FROM {schema}.committee c
        WHERE LOWER(h.name) = LOWER(c.name)
        AND h.chamber_id = c.chamber_id
        AND h.committee_id IS NULL
    """.format(
        schema=SNAPSHOT_SCHEMA, 
        hearings=HEARINGS_TABLE
        )
    
    cur.execute(update_query)
    logger.info(f"Updated committee IDs where name match found: {cur.rowcount} rows affected")
    return


def stage_hearing_bills(cur, hearing_bills_data):
    create_query = """
        CREATE TEMPORARY TABLE {stage} (
            chamber_id INT,
            event_date DATE,
            event_text TEXT,
            event_time_verbatim TEXT,
            event_location TEXT,
            event_room TEXT,
            bill_number TEXT,
            file_order INT,
            footnote TEXT,
            footnote_symbol CHAR
        );
    """.format(stage=INCOMING_HEARING_BILLS_TABLE)
    cur.execute(create_query)

    insert_query = """
        INSERT INTO {stage} (
            chamber_id, 
            event_date, 
            event_text,  
            event_time_verbatim, 
            event_location, 
            event_room,
            bill_number, 
            file_order,
            footnote,
            footnote_symbol
        )
        VALUES (%s, %s::DATE, %s, %s, %s, %s, %s, %s::INT, %s::TEXT, %s::CHAR)
    """.format(stage=INCOMING_HEARING_BILLS_TABLE)

    cur.executemany(insert_query, hearing_bills_data)
    logger.info(f"Staged hearing-bill associations: {cur.rowcount} rows affected")
    return


def upsert_hearing_bills(cur):
    upsert_query = """
        WITH resolved AS (
            SELECT
                h.hearing_id, 
                b.openstates_bill_id,
                s.file_order,
                s.footnote,
                s.footnote_symbol
            FROM {stage} s
            JOIN {schema}.{hearings} h
                ON s.chamber_id           = h.chamber_id
                AND s.event_date          = h.date
                AND s.event_text          = h.name
                AND s.event_time_verbatim = h.time_verbatim
                AND s.event_location      = h.location
                AND s.event_room          = h.room
            JOIN {schema}.bill b
                ON s.bill_number = b.bill_num
                AND b.session = '{session}'
        )
        INSERT INTO {schema}.{hearing_bills} (
            hearing_id,
            openstates_bill_id,
            file_order,
            footnote,
            footnote_symbol
        )
        SELECT
            hearing_id,
            openstates_bill_id,
            file_order,
            footnote,
            footnote_symbol
        FROM resolved
        ON CONFLICT (hearing_id, openstates_bill_id) DO UPDATE SET
            file_order      = EXCLUDED.file_order,
            footnote        = EXCLUDED.footnote,
            footnote_symbol = EXCLUDED.footnote_symbol
    """.format(
            stage=INCOMING_HEARING_BILLS_TABLE,
            schema=SNAPSHOT_SCHEMA,
            hearings=HEARINGS_TABLE,
            session=CURRENT_SESSION,
            hearing_bills=HEARING_BILLS_TABLE
        )
    
    cur.execute(upsert_query)
    logger.info(f"Upserted hearing-bill associations: {cur.rowcount} rows affected")
    return


def delete_removed_hearing_bills(cur):
    delete_query = """
        DELETE FROM {schema}.{hearing_bills} hb
        USING {schema}.{hearings} h
        WHERE hb.hearing_id = h.hearing_id
        AND EXISTS (
            SELECT 1 FROM {stage} s
            WHERE s.chamber_id        = h.chamber_id
            AND s.event_date          = h.date
            AND s.event_text          = h.name
            AND s.event_time_verbatim = h.time_verbatim
            AND s.event_location      = h.location
            AND s.event_room          = h.room
        )
        AND NOT EXISTS (
            SELECT 1 FROM {stage} s
            JOIN {schema}.bill b
                ON s.bill_number = b.bill_num
                AND b.session = '{session}'
            WHERE b.openstates_bill_id = hb.openstates_bill_id
            AND s.chamber_id = h.chamber_id
            AND s.event_date = h.date
            AND s.event_text = h.name
        )
    """.format(
            schema=SNAPSHOT_SCHEMA,
            hearing_bills=HEARING_BILLS_TABLE,
            hearings=HEARINGS_TABLE,
            stage=INCOMING_HEARING_BILLS_TABLE,
            session=CURRENT_SESSION
        )
    
    cur.execute(delete_query)
    logger.info(f"Deleted hearing-bill associations if lost from incoming: {cur.rowcount} rows affected")
    return


def log_dropped_hearing_bills(cur):
    # Log rows that failed to join on either hearing or bill
    log_query = """
        SELECT s.bill_number, s.event_text, s.event_date, s.chamber_id
        FROM {stage} s
        WHERE NOT EXISTS (
            SELECT FROM {schema}.{hearings} h
            WHERE s.chamber_id = h.chamber_id
            AND s.event_date = h.date
            AND s.event_text = h.name
            AND s.event_time_verbatim = h.time_verbatim
            AND s.event_location = h.location
            AND s.event_room = h.room
        )
        OR NOT EXISTS (
            SELECT FROM {schema}.bill b
            WHERE s.bill_number = b.bill_num
            AND b.session = '{session}'
        );
    """.format(
            stage=INCOMING_HEARING_BILLS_TABLE,
            schema=SNAPSHOT_SCHEMA,
            hearings=HEARINGS_TABLE,
            session=CURRENT_SESSION
        )
    
    cur.execute(log_query)
    dropped = cur.fetchall()
    if dropped:
        logger.warning(
            f"{len(dropped)} hearing bill rows could not be matched and were dropped:"
        )
        for row in dropped:
            logger.info(f"  bill={row[0]}, hearing={row[1]}, date={row[2]}, chamber={row[3]}")
    else:
        logger.info("All hearing bill rows matched successfully")
    return


DEADLINE_LEAD_DAYS = 7
DEADLINE_TYPE = "letter"


def upsert_hearing_deadlines(
        cur,
        lead_days=DEADLINE_LEAD_DAYS,
        deadline_type=DEADLINE_TYPE
):
    update_stale_query = """
        UPDATE {schema}.hearing_deadlines hd
        SET deadline_date = h.date - INTERVAL '{lead} days'
        FROM {schema}.hearings h
        WHERE hd.hearing_id = h.hearing_id
        AND hd.deadline_type = %s
        AND hd.deadline_date != h.date - INTERVAL '{lead} days'
    """.format(
            schema=SNAPSHOT_SCHEMA,
            lead=lead_days
        )
    
    cur.execute(update_stale_query, (deadline_type,))
    logger.info(f"Updated stale hearing deadlines: {cur.rowcount} rows affected")

    insert_query = """
        INSERT INTO {schema}.hearing_deadlines (
            hearing_id, deadline_date, deadline_type
        )
        SELECT
            h.hearing_id,
            h.date - INTERVAL '{lead} days' AS deadline_date,
            %s AS deadline_type
        FROM {schema}.hearings h
        ON CONFLICT ON CONSTRAINT unique_deadline DO NOTHING;
    """.format(
            schema=SNAPSHOT_SCHEMA,
            lead=lead_days
        )
    
    cur.execute(insert_query, (deadline_type,))

    logger.info(
        (
            f"Inserted new hearing deadlines ({lead_days} day lead, " 
            f"type='{deadline_type}'): {cur.rowcount} rows affected"
        )
    )
    return


def update(cur, hearings_data, hearing_bills_data):
    # -- Phase 1: Hearing-level changes
    stage_incoming_hearings(cur, hearings_data)
    upsert_hearings(cur)
    cancel_missing_hearings(cur)
    # -- Phase 2: Update derived fields
    # NOTE: this will be incompatible with future joint committee parsing
    update_hearing_committee_ids(cur)
    # -- Phase 3: Bill-level changes (hearing associations + deadlines)
    stage_hearing_bills(cur, hearing_bills_data)
    log_dropped_hearing_bills(cur)
    delete_removed_hearing_bills(cur)
    upsert_hearing_bills(cur)
    upsert_hearing_deadlines(cur)
    return

if __name__ == "__main__":
    import pickle
    from pathlib import Path

    CACHE_PATH = Path("snapshots/dev_cache.pkl")

    if CACHE_PATH.exists():
        with open(CACHE_PATH, "rb") as f:
            hearings, bills = pickle.load(f)
        logger.info("Loaded source data from cache")
    else:
        hearings, bills = fetch_updates()
        with open(CACHE_PATH, "wb") as f:
            pickle.dump((hearings, bills), f)
        logger.info("Fetched and cached source data")

    with db.get_cursor() as cur:
        update(cur, hearings, bills)