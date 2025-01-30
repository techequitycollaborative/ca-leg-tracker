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

# Index into credentials.ini for DB schema names
OPENSTATES_SCHEMA = config('postgresql_schemas')['openstates_schema']
LEGTRACKER_SCHEMA = config('postgresql_schemas')['legtracker_schema']

LAST_UPDATED_DEFAULT = '2000-01-01T00:00:00'

# Define columns for OpenStates API requests
PEOPLE_COLUMNS = ['openstates_people_id', 'name', 'party', 'updated_at']
ROLE_COLUMNS = ['openstates_people_id', 'org_classification', 'district']
# COMMITTEE_COLUMNS = ['openstates_committee_id', 'name', 'webpage_link']
# MEMBERSHIP_COLUMNS = ['openstates_committee_id', 'openstates_people_id', 'role']

def get_buffer(df):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep='\t', quoting=csv.QUOTE_NONE)
    buffer.seek(0)
    return buffer

def legtracker_update(cur, updated_since, force_update=False):
    truncate_query = 'TRUNCATE TABLE {0}.{1}'

    count_query = """
        SELECT COUNT(*)
        FROM {0}.people
        WHERE updated_at > '{1}'
    """

    people_query = """
        INSERT INTO {0}.test_legislator
         (
            openstates_people_id
            , chamber_id
            , name
            , district
            , party
        )
        SELECT p.openstates_people_id
            , CASE WHEN pr.org_classification = 'lower' THEN 1 ELSE 2 END AS chamber_id
            , p.name AS name
            , pr.district AS district
            , p.party as PARTY
        FROM {1}.people p
        JOIN (
            SELECT openstates_people_id
                , org_classification
                , district
            FROM {1}.people_roles 
        ) pr ON p.openstates_people_id = pr.openstates_people_id
        WHERE p.updated_at > '{2}'
        ON CONFLICT (openstates_people_id) DO UPDATE SET
            chamber_id = EXCLUDED.chamber_id,
            name = EXCLUDED.name,
            district = EXCLUDED.district,
            party = EXCLUDED.party
    """
    stamp = updated_since

    if force_update:
        print("Force update enabled. Initiating forced refresh of legtracker from snapshot...")
        stamp = LAST_UPDATED_DEFAULT
    else:
        print("Normal update. Applying changes since last update...")
    
    cur.execute(count_query.format(OPENSTATES_SCHEMA, stamp))
    count_result = cur.fetchone()
    snapshot_count = count_result[0]
    print("Retrieved {0} rows from Openstates snapshot...".format(snapshot_count))
    cur.execute(truncate_query.format(LEGTRACKER_SCHEMA, 'test_legislator'))
    cur.execute(people_query.format(LEGTRACKER_SCHEMA, OPENSTATES_SCHEMA, stamp))

def openstates_upsert_people(cur, people):
    temp_table_name = 'people_temp'
    temp_table_query = """
        CREATE TEMPORARY TABLE {0} AS
        SELECT *
        FROM {1}.people
        WHERE false
    """ # assumes people table exists

    cur.execute(temp_table_query.format(temp_table_name, OPENSTATES_SCHEMA))

    buffer = StringIO()

    people.to_csv(buffer, index=False, header=False, sep='\t', quoting=csv.QUOTE_NONE)
    buffer.seek(0)

    cur.copy_from(file=buffer, table='people_temp', sep='\t', columns=PEOPLE_COLUMNS)

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

def openstates_update_people_data(cur, people_list=[], people_roles=[]):
    table_name = 'people_roles'

    temp_table_query = """
        CREATE TEMPORARY TABLE {0}_temp AS
        SELECT *
        FROM {1}.{0}
        WHERE false
    """
    cur.execute(temp_table_query.format(table_name, OPENSTATES_SCHEMA))

    cur.copy_from(file=get_buffer(people_roles), table=table_name+'_temp', sep='\t', columns=ROLE_COLUMNS)
    
    people_id_string = "'" + "','".join(people_list) + "'"
    delete_query = """
        DELETE FROM {0}.{1}
        WHERE openstates_people_id IN ({2})
    """
    cur.execute(delete_query.format(OPENSTATES_SCHEMA, table_name, people_id_string))
    
    update_data_query = """
        INSERT INTO {0}.{1}
        SELECT *
        FROM {2}
    """
    cur.execute(update_data_query.format(OPENSTATES_SCHEMA, table_name, table_name+'_temp'))
    return

def get_last_update_timestamp(cur):
    """
    Input: psycopg2 cursor
    Output: timestamp string

    Retrieves a timestamp of the most recently updated person, or default value
    """
    query = 'SELECT MAX(updated_at) FROM {0}.people'

    cur.execute(query.format(OPENSTATES_SCHEMA))
    last_updated = cur.fetchone()[0]

    if last_updated == '' or last_updated == None:
        last_updated = LAST_UPDATED_DEFAULT

    return last_updated

def fetch_legislator_updates(updated_since=LAST_UPDATED_DEFAULT, max_page=1000, start_page=1):
    df_people = pd.DataFrame(columns=PEOPLE_COLUMNS)
    df_people_roles = pd.DataFrame(columns=ROLE_COLUMNS)

    current_page = start_page - 1
    num_pages = start_page

    while current_page < num_pages and current_page < max_page:
        current_page += 1 # increment index
        assembly_data, num_pages = people.get_assembly_data(page=current_page, updated_since=updated_since)
        print(f'Finished fetching page {current_page} of {num_pages} of assembly updates')
        
        df_people = pd.concat([df_people, pd.DataFrame(data=assembly_data['people'], columns=PEOPLE_COLUMNS)], ignore_index=True)
        df_people_roles = pd.concat([df_people_roles, pd.DataFrame(data=assembly_data['people_roles'], columns=ROLE_COLUMNS)], ignore_index=True)

    current_page = start_page - 1
    num_pages = start_page

    while current_page < num_pages and current_page < max_page:
        current_page += 1 # increment index
        senate_data, num_pages = people.get_senate_data(page=current_page, updated_since=updated_since)
        print('Finished fetching page {} of {} of senate updates'.format(current_page, num_pages))
        
        df_people = pd.concat([df_people, pd.DataFrame(data=senate_data['people'], columns=PEOPLE_COLUMNS)], ignore_index=True)
        df_people_roles = pd.concat([df_people_roles, pd.DataFrame(data=senate_data['people_roles'], columns=ROLE_COLUMNS)], ignore_index=True)

    return {
        'people': df_people,
        'people_roles': df_people_roles
    }

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

def main():
    # argument parser detects optional flags
    parser = argparse.ArgumentParser(description="Take new snapshot of OpenStates data and wrangle into legislation tracker.")
    parser.add_argument('--force-update', action='store_true', help='Force update on front-end schema without date filtering.')
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
        print('Last update timestamp: ' + last_update)
        print('Current timestamp: ' + datetime.now().strftime("%Y-%m-%d, %H:%M:%S") + " -- fetching updates from Openstates")
        legislator_updates = fetch_legislator_updates(last_update)
        # committee_updates = fetch_committee_updates(last_update)
        
        print('Summary of legislators being updated:')

        # Empty OpenStates response case
        if len(legislator_updates['people'].index) == 0:
            print("Empty legislator response from Openstates API.")

            # If flag is activated, re-sync DB schemas regardless of update
            if args.force_update: 
                legtracker_update(cur, last_update, force_update=args.force_update)
            else:
                print("Skipping forced refresh -- no legislators to update; finishing.")
        else:

            # Logs new bills to console
            print(legislator_updates['people'])
            print(legislator_updates['people_roles'])
            print("Openstates response received. Updating Openstates snapshot...")

            # Upserts new bills and new bill content into Openstates/snapshot schema
            openstates_upsert_people(cur, legislator_updates['people'])
            
            # Updates people roles from Openstates response
            openstates_update_people_data(
                cur=cur, 
                people_list=legislator_updates['people']['openstates_people_id'], 
                people_roles=legislator_updates['people_roles']
            )
            print('Legislator snapshot updated')

            # # Updates legislator table
            print('Updating legtracker tables')
            legtracker_update(cur, last_update, force_update=args.force_update)
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Failed to update records", error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed')

    print('Update finished')


if __name__ == "__main__":
    main()