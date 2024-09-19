import assembly_dailyfile_scraper as assembly
import senate_dailyfile_scraper as senate
import pandas as pd
from config import config
import psycopg2

LEGTRACKER_SCHEMA = config('postgresql_schemas')['legtracker_schema']
BILL_SCHEDULE_COLUMNS = ['bill_schedule_id', 'bill_id', 'event_date', 'event_text']
SNAPSHOT_SCHEMA = config('postgresql_schemas')['snapshot_schema'] 


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
    truncate_query = 'TRUNCATE TABLE {0}.{1}'
    # Truncate bill_schedule
    cur.execute(truncate_query.format(LEGTRACKER_SCHEMA, main_table))
    return

def stage_new_schedule(cur, schedule_data):
    # Create staging table
    temp_table_name = "new_bill_schedule"
    temp_table_query = """
        CREATE TEMPORARY TABLE {0} (
        bill_number TEXT,
        event_date DATE,
        event_text TEXT
        );
    """
    cur.execute(temp_table_query.format(temp_table_name))

    # Insert data into staging table
    insert_query = """
        INSERT INTO {0} (event_date, event_text, bill_number)
        VALUES (%s::DATE, %s, %s)
    """

    # Execute the insert query for each row in the schedule_data
    for row in schedule_data:
        cur.execute(insert_query.format(temp_table_name), (row[0], row[1], row[2]))
    return

def join_filter_ids(cur):
    # Join on bill_number and filter if bill_id is not found
    temp_table_name = "new_bill_schedule_with_ids"
    temp_table_query = """
        CREATE TABLE {0} AS
        SELECT nbs.event_date, nbs.event_text, b.bill_id
        FROM new_bill_schedule nbs
        JOIN {1}.bill b ON nbs.bill_number = b.bill_number
        WHERE b.bill_id IS NOT NULL;
    """
    cur.execute(temp_table_query.format(temp_table_name, LEGTRACKER_SCHEMA))
    return

def insert_new_schedule(cur):
    # Insert data into bill_schedule
    insert_query = """
        INSERT INTO {0}.bill_schedule (bill_id, event_date, event_text)
        SELECT sw.bill_id, sw.event_date, sw.event_text
        FROM new_bill_schedule_with_ids sw
        WHERE NOT EXISTS (
            SELECT 1 FROM {0}.bill_schedule bs 
            WHERE bs.bill_id = sw.bill_id
            AND bs.event_date = sw.event_date
            AND bs.event_text = sw.event_text
            );
    """
    cur.execute(insert_query.format(LEGTRACKER_SCHEMA))
    return

def remove_staging_table(cur):
    # Remove staging tables
    drop_query = """
        DROP TABLE IF EXISTS {};
    """
    cur.execute(drop_query.format("new_bill_schedule"))
    cur.execute(drop_query.format("new_bill_schedule_with_ids"))
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
    return assembly_update, senate_update


def main():
    conn = None
    try:
        # read connection parameters
        params = config("postgresql")

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        schedule_updates = fetch_schedule_update()

        print('Summary of schedules being updated:')
        print(f'{len(schedule_updates[0])} upcoming Assembly events')
        print(f'{len(schedule_updates[1])} upcoming Senate events')

        if len(schedule_updates[0]) > 0:
            # update bill_schedule table
            print('Updating Assembly schedule in legtracker')
            legtracker_update(cur, schedule_updates[0])
        else:
            print('No Assembly schedule updates; finishing')
        
        if len(schedule_updates[1]) > 0:
            # update bill_schedule table
            print('Updating Senate schedule in legtracker')
            legtracker_update(cur, schedule_updates[1])
        else:
            print('No Senate schedule updates; finishing')

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
