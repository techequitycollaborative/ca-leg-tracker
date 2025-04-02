"""
Connects to postgreSQL database, stages and inserts new rows to upcoming events for CA Senate and State Assembly.

After pruning events in the past from the bill_schedule table, stage new events in a temporary table. Join these rows
on their internal bill_id (and filter these rows out if a bill_id cannot be found). Insert these verified rows into
the live bill_schedule table and remove all staging tables.
"""

import dailyfile_assembly_scraper as assembly
import dailyfile_senate_scraper as senate
import pandas as pd
from config import config
import psycopg2

LEGTRACKER_SCHEMA = config("postgresql_schemas")["legtracker_schema"]
SNAPSHOT_SCHEMA = config("postgresql_schemas")["snapshot_schema"]
CURRENT_SESSION = "20252026"

# global vars for table names used across stages
MAIN_TABLE = "bill_schedule"
STAGE_OLD_TABLE = "stage_old_" + MAIN_TABLE
STAGE_NEW_TABLE = "stage_new_" + MAIN_TABLE
STAGE_ID_TABLE = "stage_id_" + MAIN_TABLE

def copy_temp_table(cur, temp_table_name):
    print("Writing table {} to CSV for review...".format(temp_table_name))

    outputquery = """
        COPY (SELECT * FROM {}) TO STDOUT WITH CSV HEADER
    """

    with open('{0}.csv'.format(temp_table_name), 'w+') as f:
        cur.copy_expert(outputquery.format(temp_table_name), f)
    return

def count_table_rows(cur, table_name):
    # Query to count rows where bill_id is NULL
    count_query = """
        SELECT COUNT(*)
        FROM {0}
    """
    cur.execute(count_query.format(table_name))
    row_count = cur.fetchone()[0]
    return row_count

def prune_bill_schedule(cur):
    # Create staging table for events that are still upcoming
    temp_table_query = """
        CREATE TEMPORARY TABLE {0} AS
        SELECT * FROM {1}.{2}
        WHERE event_date >= CURRENT_DATE;
    """
    cur.execute(temp_table_query.format(STAGE_OLD_TABLE, LEGTRACKER_SCHEMA, MAIN_TABLE))

    # Truncate query
    truncate_query = "TRUNCATE TABLE {0}.{1}"
    # Truncate bill_schedule
    cur.execute(truncate_query.format(LEGTRACKER_SCHEMA, MAIN_TABLE))

    # copy_temp_table(cur, temp_table_name)
    return


def stage_new_schedule(cur, schedule_data):
    # Create staging table
    temp_table_query = """
        CREATE TEMPORARY TABLE {0} (
        chamber_id INT,
        bill_number TEXT,
        event_date DATE,
        event_text TEXT
        );
    """
    cur.execute(temp_table_query.format(STAGE_NEW_TABLE))
    print("Staging table created...")

    # Insert data into staging table
    insert_query = """
        INSERT INTO {0} (chamber_id, event_date, event_text, bill_number)
        VALUES (%s, %s::DATE, %s, %s)
    """
    # Execute the insert query for each row in the schedule_data
    for row in schedule_data:
        cur.execute(
            insert_query.format(STAGE_NEW_TABLE), (row[0], row[1], row[2], row[3])
        )
    print("New events staged...")

    row_count = count_table_rows(cur, STAGE_NEW_TABLE)
    print(f"Number of rows inserted into {STAGE_NEW_TABLE}: {row_count}")

    # copy_temp_table(cur, temp_table_name)
    return


def join_filter_ids(cur):
    # Join on bill_number and filter if bill_id is not found
    temp_table_query = """
        CREATE TEMPORARY TABLE {0} AS
        SELECT nbs.chamber_id, b.openstates_bill_id, nbs.event_date, nbs.event_text
        FROM {1} nbs
        JOIN {2}.bill b ON nbs.bill_number = b.bill_num
        AND b.session = '{3}';
    """
    cur.execute(
        temp_table_query.format(
            STAGE_ID_TABLE,
            STAGE_NEW_TABLE,
            SNAPSHOT_SCHEMA,
            CURRENT_SESSION
        )
    )

    bill_id_count = count_table_rows(cur, STAGE_ID_TABLE)
    print(f"Number of rows staged with bill IDs: {bill_id_count}")

    # test_table_name = "new_bills_left_outer"
    # test_query = """
    #     CREATE TEMPORARY TABLE {0} AS
    #     SELECT nbs.bill_number, b.openstates_bill_id, nbs.event_date, nbs.event_text
    #     FROM new_bill_schedule nbs
    #     LEFT OUTER JOIN {1}.bill b 
    #         ON nbs.bill_number = b.bill_num
    #         AND b.session='{2}';
    # """
    # copy_temp_table(cur, temp_table_name)

    # cur.execute(
    #     test_query.format(test_table_name, SNAPSHOT_SCHEMA, CURRENT_SESSION)
    # )

    # copy_temp_table(cur, test_table_name)
    return


def insert_new_schedule(cur):
    # Insert data into bill_schedule and update event text with newer data on conflict
    insert_query = """
        INSERT INTO {0}.{1} (chamber_id, event_date, event_text, openstates_bill_id)
        SELECT sw.chamber_id, sw.event_date, sw.event_text, sw.openstates_bill_id
        FROM {2} sw
        ON CONFLICT (openstates_bill_id, chamber_id, event_date)
        DO UPDATE SET
            event_text = EXCLUDED.event_text;
    """
    cur.execute(insert_query.format(LEGTRACKER_SCHEMA, MAIN_TABLE, STAGE_ID_TABLE))
    print("New events inserted...")
    return


def remove_staging_table(cur):
    # Remove staging tables
    drop_query = """
        DROP TABLE IF EXISTS {};
    """
    cur.execute(drop_query.format(STAGE_ID_TABLE))
    cur.execute(drop_query.format(STAGE_NEW_TABLE))
    cur.execute(drop_query.format(STAGE_OLD_TABLE))
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
