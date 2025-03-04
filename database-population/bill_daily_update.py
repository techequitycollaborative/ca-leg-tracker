"""
This script executes daily updates for both the snapshot (OpenStates) and front-end (legtracker) schemas.

Input: optional flag for forced update when schemas are out of sync
Output: None; database update messages printed to console/log.

First, fetch each page of possible updates from OpenStates and write to a Pandas dataframe. The first
stage of manipulation is upserting a 'snapshot' of Openstates data to the snapshot schema. Next,
this snapshot schema's content is wrangled into the front-end tables. Database manipulation is done by
executing SQL queries with formatted Python strings and psycopg2 commands.

Note: SQL query logic has unresolved bugs for updating bill_history table
"""

import argparse
import bill_openstates_fetch as openstates
import pandas as pd
from config import config
import psycopg2
from io import StringIO
import csv
from datetime import datetime

# Index into credentials.ini for DB schema names
OPENSTATES_SCHEMA = config("postgresql_schemas")["openstates_schema"]
LEGTRACKER_SCHEMA = config("postgresql_schemas")["legtracker_schema"]

LAST_UPDATED_DEFAULT = "2000-01-01T00:00:00"

# Define columns for OpenStates API requests
BILL_COLUMNS = [
    "openstates_bill_id",
    "session",
    "chamber",
    "bill_num",
    "title",
    "created_at",
    "updated_at",
    "first_action_date",
    "last_action_date",
    "abstract",
]
BILL_ACTION_COLUMNS = [
    "openstates_bill_id",
    "chamber",
    "description",
    "action_date",
    "action_order",
]
BILL_SPONSOR_COLUMNS = [
    "openstates_bill_id",
    "name",
    "full_name",
    "title",
    "district",
    "primary_author",
    "type",
]
BILL_VOTE_COLUMNS = [
    "openstates_bill_id",
    "motion_text",
    "vote_date",
    "vote_location",
    "vote_result",
    "vote_threshold",
    "yes_count",
    "no_count",
    "other_count",
]


def openstates_upsert_bills(cur, bills):
    """
    Input: psycopg2 cursor, array of bill data in Openstates structure
    Output: None (creates temporary CSV and executes SQL queries)

    After writing all new bills to a temp CSV file, use temp SQL table to upsert live bills table
    in the OPENSTATES_SCHEMA
    """

    # Create a temporary bill table by copying OPENSTATES_SCHEMA.bill columns
    # WHERE false clause is shortcut to creating empty temporary table
    temp_table_name = "bill_temp"
    temp_table_query = """
        CREATE TEMPORARY TABLE {0} AS
        SELECT *
        FROM {1}.bill
        WHERE false
    """
    cur.execute(temp_table_query.format(temp_table_name, OPENSTATES_SCHEMA))

    # Create a string buffer
    buffer = StringIO()

    # Given list of bills fetched from OpenStates, write to buffer
    bills.to_csv(buffer, index=False, header=False, sep="\t", quoting=csv.QUOTE_NONE)
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
    cur.execute(update_bills_query.format(OPENSTATES_SCHEMA, temp_table_name))


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
    cur.execute(temp_table_query.format(bill_action, OPENSTATES_SCHEMA))
    cur.execute(temp_table_query.format(bill_sponsor, OPENSTATES_SCHEMA))
    cur.execute(temp_table_query.format(bill_vote, OPENSTATES_SCHEMA))

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
    cur.execute(delete_query.format(OPENSTATES_SCHEMA, bill_action, bill_ids_string))
    cur.execute(delete_query.format(OPENSTATES_SCHEMA, bill_sponsor, bill_ids_string))
    cur.execute(delete_query.format(OPENSTATES_SCHEMA, bill_vote, bill_ids_string))

    # Copy new data to live tables
    update_data_query = """
        INSERT INTO {0}.{1}
        SELECT *
        FROM {2}
    """
    cur.execute(
        update_data_query.format(OPENSTATES_SCHEMA, bill_action, bill_action + "_temp")
    )
    cur.execute(
        update_data_query.format(
            OPENSTATES_SCHEMA, bill_sponsor, bill_sponsor + "_temp"
        )
    )
    cur.execute(
        update_data_query.format(OPENSTATES_SCHEMA, bill_vote, bill_vote + "_temp")
    )


def get_buffer(df):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep="\t", quoting=csv.QUOTE_NONE)
    buffer.seek(0)
    return buffer


# Updates main legtracker tables with new openstates data
def legtracker_update(cur, updated_since, force_update=False):
    """
    Input: psycopg2 cursor, timestamp, forced update flag value
    Output: None (updates live front-end tables)

    Updates front-end tables bill, bill_history, chamber_vote_results with new Openstates data.
    """

    ### QUERIES ###

    # Truncate query
    truncate_query = "TRUNCATE TABLE {0}.{1}"

    # Count query before upsert
    # JOIN clause selects bill_sponsor rows with valid authorship types and coalesces possible columns for names
    count_query = """
        SELECT COUNT(*)
        FROM {0}.bill
        JOIN (
            SELECT openstates_bill_id
                , coalesce(full_name,name) AS name
            FROM {0}.bill_sponsor
            WHERE primary_author = 'True'
        ) a1 USING(openstates_bill_id)
        LEFT JOIN (
            SELECT openstates_bill_id
                , string_agg(name, ', ') AS names
            FROM {0}.bill_sponsor
            WHERE primary_author != 'True'
              AND type IN ('author', 'principal coauthor')
            GROUP BY 1
        ) a2 USING(openstates_bill_id)
        WHERE updated_at > '{1}'
    """
    # Upsert query for bill table
    # INSERT clause selects columns from LEGTRACKER.bill to update
    # SELECT clause maps OPENSTATES_SCHEMA.bill column names to LEGTRACKER.bill and generates leginfo URL from template
    # JOIN clause normalizes sponsor/author names
    # WHERE clause selects by last-updated date timestamp
    # ON CONFLICT clause chooses selected content to overwrite existing rows when in conflict
    bill_query = """
        INSERT INTO {0}.bill (
            openstates_bill_id
            , bill_name
            , bill_number
            , full_text
            , author
            , origin_chamber_id
            , committee_id
            , status
            , coauthors
            , leginfo_link
            , leg_session
        )
        SELECT a1.openstates_bill_id
            , b.title AS bill_name
            , b.bill_num AS bill_number
            , b.abstract AS full_text
            , a1.name AS author
            , CASE WHEN b.chamber = 'Assembly' THEN 1 ELSE 2 END AS origin_chamber_id
            , null AS committee_id
            , null AS status
            , a2.names AS coauthors
            , CONCAT('https://leginfo.legislature.ca.gov/faces/billTextClient.xhtml?bill_id=202320240',REPLACE(b.bill_num, ' ', '')) AS leginfo_link
            , b.session AS leg_session
        FROM {1}.bill b
        JOIN (
            SELECT openstates_bill_id
                , coalesce(full_name,name) AS name
            FROM {1}.bill_sponsor
            WHERE primary_author = 'True'
        ) a1 USING(openstates_bill_id)
        LEFT JOIN (
            SELECT openstates_bill_id
                , string_agg(name, ', ') AS names
            FROM {1}.bill_sponsor
            WHERE primary_author != 'True'
              AND type IN ('author', 'principal coauthor')
            GROUP BY 1
        ) a2 USING(openstates_bill_id)
        WHERE b.updated_at > '{2}'
        ON CONFLICT (openstates_bill_id) DO UPDATE SET
            bill_name = EXCLUDED.bill_name,
            bill_number = EXCLUDED.bill_number,
            full_text = EXCLUDED.full_text,
            author = EXCLUDED.author,
            origin_chamber_id = EXCLUDED.origin_chamber_id,
            committee_id = EXCLUDED.committee_id,
            status = EXCLUDED.status,
            coauthors = EXCLUDED.coauthors,
            leginfo_link = EXCLUDED.leginfo_link,
            leg_session = EXCLUDED.leg_session
    """

    # Rebuild bill history table query
    # INSERT clause selects columns from LEGTRACKER.bill_history to update
    # SELECT clause maps columns from OPENSTATES_SCHEMA.bill_action
    # JOIN clause concatenates OPENSTATES_SCHEMA.bill data to selection
    # JOIN clause concatenates LEGTRACKER.bill data to selection to index on internal bill_id
    bill_history_query = """
        INSERT INTO {0}.bill_history (
            bill_id
            , event_date
            , event_text
            , chamber_id
            , history_order
        )
        SELECT b2.bill_id
            , a.action_date::date AS event_date
            , a.description AS event_text
            , CASE WHEN a.chamber = 'Assembly' THEN 1
                   WHEN a.chamber = 'Senate' THEN 2
                   ELSE null END AS chamber_id
            , a.action_order::integer AS history_order
        FROM {1}.bill_action a
        JOIN {1}.bill b1 USING(openstates_bill_id)
        JOIN {0}.bill b2 ON b1.bill_num = b2.bill_number
    """

    # Rebuild vote result table query
    # INSERT clause selects columns to update
    # SELECT clause selects columns from OPENSTATES_SCHEMA.bill_vote
    # JOIN clause concatenates bill data from OPENSTATES_SCHEMA.bill
    # JOIN clause concatenates bill data from LEGTRACKER.bill ??
    # JOIN clause concatenates chamber data from LEGTRACKER.chamber
    bill_vote_query = """
        INSERT INTO {0}.chamber_vote_result(
            vote_date
            , bill_id
            , chamber_id
            , vote_text
            , vote_threshold
            , vote_result
            , votes_for
            , votes_against
            , votes_other
        )
        SELECT a.vote_date::date
            , c.bill_id
            , d.chamber_id
            , a.motion_text
            , a.vote_threshold
            , a.vote_result
            , a.yes_count::integer
            , a.no_count::integer
            , a.other_count::integer
        FROM {1}.bill_vote a
        JOIN {1}.bill b USING(openstates_bill_id)
        JOIN {0}.bill c ON b.bill_num = c.bill_number
        JOIN {0}.chamber d ON a.vote_location = d.name
    """

    ### RUN UPDATES ###

    # Set default value for timestamp
    stamp = updated_since

    # Update timestamp if forced update flag is TRUE
    if force_update:
        print(
            "Force update enabled. Initiating forced refresh of legtracker from snapshot..."
        )
        stamp = LAST_UPDATED_DEFAULT
    else:
        print("Normal update. Applying changes since last update...")

    # Upsert changes into bill table
    cur.execute(count_query.format(OPENSTATES_SCHEMA, stamp))
    count_result = cur.fetchone()
    snapshot_count = count_result[0]
    print("Retrieved {0} rows from Openstates snapshot...".format(snapshot_count))
    cur.execute(bill_query.format(LEGTRACKER_SCHEMA, OPENSTATES_SCHEMA, stamp))

    # Clear and rebuild bill history and votes tables
    cur.execute(truncate_query.format(LEGTRACKER_SCHEMA, "bill_history"))
    cur.execute(truncate_query.format(LEGTRACKER_SCHEMA, "chamber_vote_result"))
    cur.execute(bill_history_query.format(LEGTRACKER_SCHEMA, OPENSTATES_SCHEMA))
    cur.execute(bill_vote_query.format(LEGTRACKER_SCHEMA, OPENSTATES_SCHEMA))


def get_last_update_timestamp(cur):
    """
    Input: psycopg2 cursor
    Output: timestamp string

    Retrieves a timestamp of the most recently updated bill, or default value
    """
    query = "SELECT MAX(updated_at) FROM {0}.bill"

    cur.execute(query.format(OPENSTATES_SCHEMA))
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
                legtracker_update(cur, last_update, force_update=args.force_update)
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
            print("Snapshot updated")

            # Updates bill, bill_history, and chamber_vote_result tables
            print("Updating legtracker tables")
            legtracker_update(cur, last_update, force_update=args.force_update)
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
