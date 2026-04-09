""" """

import sources.bill_openstates_fetch as openstates
import db
import pandas as pd
from config import config
from io import StringIO
import csv
from yaml import safe_load

# Index into credentials.ini for globals
SNAPSHOT_SCHEMA = config("postgresql_schemas")["snapshot_schema"]
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


def get_last_update_timestamp():
    """
    Output: timestamp string

    Retrieves a timestamp of the most recently updated bill, or default value
    """
    query = "SELECT MAX(updated_at) FROM {0}.bill"

    with db.get_cursor() as cur:
        cur.execute(query.format(SNAPSHOT_SCHEMA))
        last_updated = cur.fetchone()[0]

    if last_updated == "" or last_updated is None:
        last_updated = LAST_UPDATED_DEFAULT

    return last_updated


def fetch_updates(updated_since=LAST_UPDATED_DEFAULT, max_page=1000, start_page=1):
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


def upsert_bill_data(cur, bills):
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


def upsert(cur, response):
    upsert_bill_data(cur, response["bills"])
    openstates_update_bill_data(
        cur,
        response["bills"]["openstates_bill_id"],
        response["bill_actions"],
        response["bill_sponsors"],
        response["bill_votes"],
    )
    return
