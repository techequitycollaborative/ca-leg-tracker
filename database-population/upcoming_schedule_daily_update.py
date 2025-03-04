"""
Connects to postgreSQL database, stages and inserts new rows to upcoming events for CA Senate and State Assembly.

After pruning events in the past from the bill_schedule table, stage new events in a temporary table. Join these rows
on their internal bill_id (and filter these rows out if a bill_id cannot be found). Insert these verified rows into
the live bill_schedule table and remove all staging tables.
"""

import assembly_dailyfile_scraper as assembly
import senate_dailyfile_scraper as senate
import pandas as pd
from config import config
import psycopg2

LEGTRACKER_SCHEMA = config("postgresql_schemas")["legtracker_schema"]
BILL_SCHEDULE_COLUMNS = [
    "bill_schedule_id",
    "bill_id",
    "chamber_id",
    "event_date",
    "event_text",
]
SNAPSHOT_SCHEMA = config("postgresql_schemas")["snapshot_schema"]
CURRENT_SESSION = "20252026"


def prune_bill_schedule(cur):
    # Create staging table for events that are still upcoming
    main_table = "bill_schedule"
    temp_table_name = "existing_bill_schedule"
    temp_table_query = """
        CREATE TEMPORARY TABLE {0} AS
        SELECT * FROM {1}.bill_schedule
        WHERE event_date >= CURRENT_DATE;
    """
    cur.execute(temp_table_query.format(temp_table_name, LEGTRACKER_SCHEMA))

    # Truncate query
    truncate_query = "TRUNCATE TABLE {0}.{1}"
    # Truncate bill_schedule
    cur.execute(truncate_query.format(LEGTRACKER_SCHEMA, main_table))
    return


def stage_new_schedule(cur, schedule_data):
    # Create staging table
    temp_table_name = "new_bill_schedule"
    temp_table_query = """
        CREATE TEMPORARY TABLE {0} (
        chamber_id INT,
        bill_number TEXT,
        event_date DATE,
        event_text TEXT
        );
    """
    cur.execute(temp_table_query.format(temp_table_name))
    print("Staging table created...")

    # Insert data into staging table
    insert_query = """
        INSERT INTO {0} (chamber_id, event_date, event_text, bill_number)
        VALUES (%s, %s::DATE, %s, %s)
    """

    count_query = """
        SELECT COUNT(*) FROM {0};
    """

    # Execute the insert query for each row in the schedule_data
    for row in schedule_data:
        cur.execute(
            insert_query.format(temp_table_name), (row[0], row[1], row[2], row[3])
        )
    print("New events staged...")

    cur.execute(count_query.format(temp_table_name))
    row_count = cur.fetchone()[0]
    print(f"Number of rows inserted into {temp_table_name}: {row_count}")
    return


def join_filter_ids(cur):
    # Join on bill_number and filter if bill_id is not found
    temp_table_name = "new_bill_schedule_with_ids"
    temp_table_query = """
        CREATE TABLE {0} AS
        SELECT nbs.chamber_id, b.bill_id, nbs.event_date, nbs.event_text
        FROM new_bill_schedule nbs
        JOIN {1}.bill b ON nbs.bill_number = b.bill_number
        AND b.leg_session = '{2}'
        WHERE b.bill_id IS NOT NULL;
    """

    # Query to count rows where bill_id is NULL
    count_query = """
        SELECT COUNT(*)
        FROM {0}
    """

    cur.execute(
        temp_table_query.format(temp_table_name, LEGTRACKER_SCHEMA, CURRENT_SESSION)
    )
    cur.execute(count_query.format(temp_table_name))
    bill_id_count = cur.fetchone()[0]
    print(f"Number of rows staged with bill IDs: {bill_id_count}")
    return


def insert_new_schedule(cur):
    # Insert data into bill_schedule and update event text with newer data on conflict
    insert_query = """
        INSERT INTO {0}.bill_schedule (bill_id, chamber_id, event_date, event_text)
        SELECT sw.bill_id, sw.chamber_id, sw.event_date, sw.event_text
        FROM new_bill_schedule_with_ids sw
        ON CONFLICT (bill_id, chamber_id, event_date)
        DO UPDATE SET
            event_text = EXCLUDED.event_text;
    """
    cur.execute(insert_query.format(LEGTRACKER_SCHEMA))
    print("New events inserted...")
    return


def remove_staging_table(cur):
    # Remove staging tables
    drop_query = """
        DROP TABLE IF EXISTS {};
    """
    cur.execute(drop_query.format("new_bill_schedule"))
    cur.execute(drop_query.format("new_bill_schedule_with_ids"))
    cur.execute(drop_query.format("existing_bill_schedule"))
    print("Dropped staging tables...")
    return


def legtracker_update(cur, schedule_data):
    prune_bill_schedule(cur)
    stage_new_schedule(cur, schedule_data)
    join_filter_ids(cur)
    insert_new_schedule(cur)
    remove_staging_table(cur)
    return


def fetch_schedule_update():
    assembly_update = assembly.scrape_dailyfile()
    senate_update = senate.scrape_dailyfile()

    print(f"{len(assembly_update)} upcoming Assembly events")
    print(f"{len(senate_update)} upcoming Senate events")

    # join sets before returning
    return assembly_update | senate_update


def main():
    conn = None
    try:
        # read connection parameters
        params = config("postgresql")

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        print("Summary of schedules being updated:")
        schedule_updates = fetch_schedule_update()

        if len(schedule_updates):
            print("Updating bill schedule for both chambers...")
            legtracker_update(cur, schedule_updates)
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
