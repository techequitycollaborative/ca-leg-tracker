# Functions to run once per session: legislators, committees, and committee assignments
import api_requests
import datetime
import psycopg2
from make_db_id import add_digit_id
from config import config
from sys import exit
year = datetime.date.today().strftime("%Y")
SESSION_YEAR = year + str(int(year) + 1)


def openstates_update():
    legislators = api_requests.get_legislator_data_openstates()
    legislators = add_digit_id(legislators, "name", "legislator_id")
    return legislators


def insert_legislators(cur, conn, legislators):
    for legislator in legislators:
        try:
            insert_query = """INSERT INTO ca.legislator 
            (legislator_id, chamber_id, name, district, party) 
            VALUES (%s, %s, %s, %s, %s)"""
            legislator_to_insert = (
                legislator["legislator_id"],
                legislator["chamber_id"],
                legislator["name"],
                legislator["district"],
                legislator["party"]
            )
            cur.execute(insert_query, legislator_to_insert)
            conn.commit()
            count = cur.rowcount
            print(count, "Legislator inserted successfully into legislator table")
        except (Exception, psycopg2.Error) as error:
            print("Failed to insert legislator into legislator table", error)
            exit(1)


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config("postgresql")
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        # create a cursor
        cur = conn.cursor()
        # clear old data from session-populated tables
        cur.execute("DELETE FROM ca.legislator")
        cur.execute("DELETE FROM ca.committee")
        cur.execute("DELETE FROM ca.committee_assignment")
        # insert legislators into table
        legislators = openstates_update()
        insert_legislators(cur, conn, legislators)
        # TODO: insert committees into table
        # TODO: map legislators to committees, insert relationships to table
    except (Exception, psycopg2.DatabaseError) as error:
        print("Failed to update records", error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def main():
    connect()


if __name__ == "__main__":
    main()
