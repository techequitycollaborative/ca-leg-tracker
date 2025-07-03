"""
This script executes updates needed for a new legislative session of the California legislature:
- Updated legislator, party, district, and role information
- Updated committee and committee assignment information

Input: optional flag for forced update when schemas are out of sync
Output: Success message of database operations printed to console/log


"""

import argparse
from config import config
import psycopg2
import pandas as pd
from datetime import datetime
from io import StringIO
import csv
import people_openstates_fetch as people
import capitol_codex_scraper as codex
from text_utils import transform_name

# Index into credentials.ini for DB schema names
OPENSTATES_SCHEMA = config("postgresql_schemas")["openstates_schema"]
LEGTRACKER_SCHEMA = config("postgresql_schemas")["legtracker_schema"]

LAST_UPDATED_DEFAULT = "2000-01-01T00:00:00"

# Define columns for OpenStates API requests
PEOPLE_COLUMNS = ["openstates_people_id", "name", "party", "updated_at"]
ROLE_COLUMNS = ["openstates_people_id", "org_classification", "district"]
OFFICE_COLUMNS = ["openstates_people_id", "name", "phone", "address", "classification"]
NAME_COLUMNS = ["openstates_people_id", "alt_name"]
SOURCE_COLUMNS = ["openstates_people_id", "source_url"]
CONTACTS_COLUMNS = ["openstates_people_id", "staffer_contact", "generated_email", "issue_area", "staffer_type"]
# COMMITTEE_COLUMNS = ['openstates_committee_id', 'name', 'webpage_link']
# MEMBERSHIP_COLUMNS = ['openstates_committee_id', 'openstates_people_id', 'role']


def get_buffer(df):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep="\t", quoting=csv.QUOTE_NONE)
    buffer.seek(0)
    return buffer


def legtracker_update(cur, updated_since, force_update=False):
    truncate_query = "TRUNCATE TABLE {0}.{1}"

    count_query = """
        SELECT COUNT(*)
        FROM {0}.people
        WHERE updated_at > '{1}'
    """

    fetch_query = """
        SELECT p.openstates_people_id
            , CASE WHEN pr.org_classification = 'lower' THEN 1 ELSE 2 END AS chamber_id
            , p.name AS name
            , pr.district AS district
            , p.party as PARTY
        FROM {0}.people p
        JOIN (
            SELECT openstates_people_id
                , org_classification
                , district
            FROM {0}.people_roles 
        ) pr ON p.openstates_people_id = pr.openstates_people_id
        WHERE p.updated_at > '{1}'
    """

    insert_query = """
        INSERT INTO {0}.legislator
        (openstates_people_id, chamber_id, name, district, party)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (openstates_people_id) DO UPDATE SET
            chamber_id = EXCLUDED.chamber_id,
            name = EXCLUDED.name,
            district = EXCLUDED.district,
            party = EXCLUDED.party
    """

    stamp = updated_since

    if force_update:
        print(
            "Force update enabled. Initiating forced refresh of legtracker from snapshot..."
        )
        stamp = LAST_UPDATED_DEFAULT
    else:
        print("Normal update. Applying changes since last update...")

    cur.execute(count_query.format(OPENSTATES_SCHEMA, stamp))
    count_result = cur.fetchone()
    snapshot_count = count_result[0]
    print("Retrieved {0} rows from Openstates snapshot...".format(snapshot_count))

    # Truncate the legislator table
    cur.execute(truncate_query.format(LEGTRACKER_SCHEMA, "legislator"))

    # Fetch the data from the snapshot schema
    cur.execute(fetch_query.format(OPENSTATES_SCHEMA, stamp))
    rows = cur.fetchall()

    # Insert the transformed data into the legislator table
    for row in rows:
        openstates_people_id, chamber_id, name, district, party = row
        transformed_name = transform_name(name)  # Apply the transformation
        cur.execute(
            insert_query.format(LEGTRACKER_SCHEMA),
            (openstates_people_id, chamber_id, transformed_name, district, party),
        )

    print("Update completed successfully.")


def openstates_upsert_people(cur, people):
    temp_table_name = "people_temp"
    temp_table_query = """
        CREATE TEMPORARY TABLE {0} AS
        SELECT *
        FROM {1}.people
        WHERE false
    """  # assumes people table exists

    cur.execute(temp_table_query.format(temp_table_name, OPENSTATES_SCHEMA))

    buffer = StringIO()

    people.to_csv(buffer, index=False, header=False, sep="\t", quoting=csv.QUOTE_NONE)
    buffer.seek(0)

    cur.copy_from(file=buffer, table="people_temp", sep="\t", columns=PEOPLE_COLUMNS)

    update_people_query = """
        INSERT INTO {0}.people
        SELECT *
        FROM {1}
        ON CONFLICT (openstates_people_id) DO UPDATE SET
            name=EXCLUDED.name,
            party=EXCLUDED.party,
            updated_at=EXCLUDED.updated_at
    """
    cur.execute(update_people_query.format(OPENSTATES_SCHEMA, temp_table_name))
    return

def load_from_buffer(cur, df, table_name, table_columns):
    cur.copy_from(
        file=get_buffer(df),
        table=table_name + "_temp",
        sep="\t",
        columns=table_columns
    )
    return

def flush_table(cur, table_name, people_id_string):
    delete_query = """
        DELETE FROM {0}.{1}
        WHERE openstates_people_id IN ({2})
    """
    print("Delete old {} snapshot".format(table_name))
    cur.execute(delete_query.format(OPENSTATES_SCHEMA, table_name, people_id_string))
    print(cur.statusmessage)
    return

def insert_from_temp(cur, table_name, table_columns):
    update_data_query = """
        INSERT INTO {0}.{1} ({2})
        SELECT {2}
        FROM {3}
    """
    cur.execute(
        update_data_query.format(
            OPENSTATES_SCHEMA, 
            table_name,
            ", ".join(table_columns), 
            table_name + "_temp"
        )
    )
    print(cur.statusmessage)
    return

def openstates_update_people_data(
        cur, 
        people_list=[], 
        people_role_data=[],
        people_office_data=[],
        people_name_data=[],
        people_source_data=[]
        ):
    
    # Temp table names
    people_roles = "people_roles"
    people_offices = "people_offices"
    people_names = "people_names"
    people_sources = "people_sources"

    temp_table_query = """
        CREATE TEMPORARY TABLE {0}_temp AS
        SELECT *
        FROM {1}.{0}
        WHERE false
    """
    # Create temporary tables
    print("Creating temporary tables...")
    cur.execute(temp_table_query.format(people_roles, OPENSTATES_SCHEMA))
    print(cur.statusmessage)
    cur.execute(temp_table_query.format(people_offices, OPENSTATES_SCHEMA))
    print(cur.statusmessage)
    cur.execute(temp_table_query.format(people_names, OPENSTATES_SCHEMA))
    print(cur.statusmessage)
    cur.execute(temp_table_query.format(people_sources, OPENSTATES_SCHEMA))
    print(cur.statusmessage)

    # Load new data
    print("Loading data from buffer...")
    load_from_buffer(cur, people_role_data, people_roles, ROLE_COLUMNS)
    load_from_buffer(cur, people_office_data, people_offices, OFFICE_COLUMNS)
    load_from_buffer(cur, people_name_data, people_names, NAME_COLUMNS)
    load_from_buffer(cur, people_source_data, people_sources, SOURCE_COLUMNS)

    # Flush old data
    print("Flushing old data from tables...")
    people_id_string = "'" + "','".join(people_list) + "'"
    flush_table(cur, people_roles, people_id_string)
    flush_table(cur, people_offices, people_id_string)
    flush_table(cur, people_names, people_id_string)
    flush_table(cur, people_sources, people_id_string)

    # Insert new rows
    print("Updating tables with new data")
    insert_from_temp(cur, people_roles, ROLE_COLUMNS)
    insert_from_temp(cur, people_offices, OFFICE_COLUMNS)
    insert_from_temp(cur, people_names, NAME_COLUMNS)
    insert_from_temp(cur, people_sources, SOURCE_COLUMNS)
    return


def get_last_update_timestamp(cur):
    """
    Input: psycopg2 cursor
    Output: timestamp string

    Retrieves a timestamp of the most recently updated person, or default value
    """
    query = "SELECT MAX(updated_at) FROM {0}.people"

    cur.execute(query.format(OPENSTATES_SCHEMA))
    last_updated = cur.fetchone()[0]

    if last_updated == "" or last_updated == None:
        last_updated = LAST_UPDATED_DEFAULT

    return last_updated


def update_df(df, new_data, table_columns):
    df = pd.concat(
            [
                df,
                pd.DataFrame(data=new_data, columns=table_columns),
            ],
            ignore_index=True,
        )
    return df

def fetch_chamber_update(
        chamber_name,
        updated_since=LAST_UPDATED_DEFAULT,
        max_page=1000,
        start_page=1
):
    df_people = pd.DataFrame(columns=PEOPLE_COLUMNS)
    df_people_roles = pd.DataFrame(columns=ROLE_COLUMNS)
    df_people_offices = pd.DataFrame(columns=OFFICE_COLUMNS)
    df_people_names = pd.DataFrame(columns=NAME_COLUMNS)
    df_people_sources = pd.DataFrame(columns=SOURCE_COLUMNS)

    current_page = start_page - 1
    num_pages = start_page

    while current_page < num_pages and current_page < max_page:
        current_page += 1 # increment
        if chamber_name == "assembly":
            chamber_data, num_pages = people.get_assembly_data(
                page=current_page, 
                updated_since=updated_since
            )
            print(
                f"Finished fetching page {current_page} of {num_pages} of assembly updates"
            )
        else:
            chamber_data, num_pages = people.get_senate_data(
                page=current_page, 
                updated_since=updated_since
            )
            print(
                f"Finished fetching page {current_page} of {num_pages} of senate updates"
            )
        
        # Update dataframes
        df_people = update_df(df_people, chamber_data["people"], table_columns=PEOPLE_COLUMNS)
        df_people_roles = update_df(df_people_roles, chamber_data["people_roles"], table_columns=ROLE_COLUMNS)
        df_people_offices = update_df(df_people_offices, chamber_data["people_offices"], table_columns=OFFICE_COLUMNS)
        df_people_names = update_df(df_people_names, chamber_data["people_names"], table_columns=NAME_COLUMNS)
        df_people_sources = update_df(df_people_sources, chamber_data["people_sources"], table_columns=SOURCE_COLUMNS)
    
    return {
        "people": df_people, 
        "people_roles": df_people_roles,
        "people_offices": df_people_offices,
        "people_names": df_people_names,
        "people_sources": df_people_sources
        }

def fetch_legislator_updates(updated_since=LAST_UPDATED_DEFAULT):
    # Get each chamber's updates
    assembly_update = fetch_chamber_update(
        "assembly", 
        updated_since=updated_since
        )
    
    senate_update = fetch_chamber_update(
        "senate",
        updated_since=updated_since
    )
    
    # Concat together the updates
    results = {}
    for k in assembly_update.keys():
        curr = pd.concat([assembly_update[k], senate_update[k]])
        assert type(curr) == pd.DataFrame
        results[k] = curr
    
    # Final results
    return results

def fetch_codex_updates():
    assembly_update = codex.extract_contacts("asm")
    senate_update = codex.extract_contacts("sen")

    # Concat together the updates from each chamber
    all_issues = set(assembly_update.keys()) | set(senate_update.keys())
    results = dict()

    for issue in all_issues:
        asm_df = assembly_update.get(issue, pd.DataFrame())
        sen_df = senate_update.get(issue, pd.DataFrame())
        results[issue] = pd.concat([asm_df, sen_df], ignore_index=True)
    
    # Final results
    return results

# def openstates_upsert_committee_data(cur, committees):
#     temp_table_name = 'committees_temp'
#     temp_table_query = """
#         CREATE TEMPORARY TABLE {0} AS
#         SELECT *
#         FROM {1}.people
#         WHERE false
#     """ # assumes committee table exists?

#     cur.execute(temp_table_query.format(temp_table_name, OPENSTATES_SCHEMA))

#     buffer = StringIO()

#     committees.to_csv(buffer, index=False, header=False, sep='\t', quoting=csv.QUOTE_NONE)
#     buffer.seek(0)

#     cur.copy_from(file=buffer, table=temp_table_name, sep='\t', columns=COMMITTEE_COLUMNS)

#     update_committees_query = """
#         INSERT INTO {0}.committees
#         SELECT *
#         FROM {1}
#         ON CONFLICT (openstates_committee_id) DO UPDATE SET
#             name=EXCLUDED.name,
#             party=EXCLUDED.party,
#             current_role=EXCLUDED.current_role,
#             updated_at=EXCLUDED.updated_at
#     """
#     cur.execute(update_committees_query.format(OPENSTATES_SCHEMA, temp_table_name))
#     return

# def fetch_committee_updates(updated_since=LAST_UPDATED_DEFAULT, max_page=1000, start_page=1):
#     df_committees = pd.DataFrame(columns=COMMITTEE_COLUMNS)
#     df_committee_membership = pd.DataFrame(columns=MEMBERSHIP_COLUMNS)

#     current_page = start_page - 1
#     num_pages = start_page

#     while current_page < num_pages and current_page < max_page:
#         committee_data, num_pages = openstates.get_committee_data(page=current_page, updated_since=updated_since)
#         print('Finished fetching page {} of {} of committee updates'.format(current_page, num_pages))

#         df_committees = pd.concat([df_committees, pd.DataFrame(data=committee_data['committees'], columns=PEOPLE_COLUMNS)], ignore_index=True)
#         df_committee_membership = pd.concat([df_committee_membership, pd.DataFrame(data=committee_data['committee_memberships'], columns=ROLE_COLUMNS)], ignore_index=True)
#         current_page += 1 # increment index

#     return {
#         'committees': df_committees,
#         'committee_membership': df_committee_membership
#     }

def codex_upsert_contacts(
        cur,
        contact_data
):
    temp_table_name = "contacts_temp"
    temp_table_query = """
        CREATE TEMPORARY TABLE {0} (
        district_number INT,
        staffer_contact TEXT,
        generated_email TEXT,
        issue_area TEXT,
        staffer_type TEXT
        )
    """  # specifying columns because they differ from the final people_contacts table
    
    cur.execute(temp_table_query.format(temp_table_name, OPENSTATES_SCHEMA))

    # Insert contacts collected for each issue to the staging table
    for issue, df in contact_data.items():
        print(f"Processing {issue}...")
        if df.empty:
            print(f"Skipping {issue} (empty DataFrame)")
            continue

        try:
            buffer = StringIO()

            df.to_csv(buffer, index=False, header=False, sep="\t", quoting=csv.QUOTE_NONE, escapechar='\\')
            buffer.seek(0)

            cur.copy_expert(
                sql="COPY {0} FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t')".format(temp_table_name),
                file=buffer
                )
            print(f"Staged {len(df)} rows for {issue}")
        except Exception as e:
                print(f"[CONTACTS] ERROR processing {issue}: {str(e)}")                
        finally:
            buffer.close()
    
    # Final bulk insert
    print("Inserting from temp to final table...")

    insert_query = """
        INSERT INTO {0}.people_contacts (openstates_people_id, staffer_contact, generated_email, issue_area, staffer_type)
        SELECT 
            pr.openstates_people_id,
            t.staffer_contact,
            t.generated_email,
            t.issue_area,
            t.staffer_type
        FROM {1} t
        JOIN {0}.people_roles pr ON t.district_number = pr.district
    """
    cur.execute(insert_query.format(OPENSTATES_SCHEMA, temp_table_name))
    print(cur.statusmessage)
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

        # GET ALL DATA
        legislator_updates = fetch_legislator_updates(last_update)
        # committee_updates = fetch_committee_updates(last_update)
        contact_updates = fetch_codex_updates()

        print("Summary of legislators being updated:")

        # Empty OpenStates response case
        if len(legislator_updates["people"].index) == 0:
            print("Empty legislator response from Openstates API.")

            # If flag is activated, re-sync DB schemas regardless of update
            if args.force_update:
                legtracker_update(cur, last_update, force_update=args.force_update)
            else:
                print("Skipping forced refresh -- no legislators to update; finishing.")
        else:

            # Logs new information to console
            for k in legislator_updates:
                print(k)
                print(legislator_updates[k])
                print()
            print("Openstates response received. Updating Openstates snapshot...")

            # Upserts new bills and new bill content into Openstates/snapshot schema
            openstates_upsert_people(cur, legislator_updates["people"])

            # Updates people roles from Openstates response
            openstates_update_people_data(
                cur=cur,
                people_list=legislator_updates["people"]["openstates_people_id"],
                people_role_data=legislator_updates["people_roles"],
                people_office_data=legislator_updates["people_offices"],
                people_name_data=legislator_updates["people_names"],
                people_source_data=legislator_updates["people_sources"]
            )

            # Update staffer contact info
            codex_upsert_contacts(
                cur=cur,
                contact_data=contact_updates
            )
            print("Legislator snapshot updated")

            # # Updates legislator table
            print("Updating legtracker tables")
            legtracker_update(cur, last_update, force_update=args.force_update)
        conn.commit()
    except psycopg2.Error as e:
        print(f"[MAIN] Database error: {e.pgerror}")
    except Exception as e:
        print(f"[MAIN] Operation failed: {str(e)}")
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed")

    print("Update finished")


if __name__ == "__main__":
    main()
