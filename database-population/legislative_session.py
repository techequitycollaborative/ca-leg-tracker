# Functions to run once per session: legislators, committees, and committee assignments
import api_requests
import committee_scraper
import datetime
import psycopg2
import make_db_id
from config import config
from sys import exit
year = datetime.date.today().strftime("%Y")
SESSION_YEAR = year + str(int(year) + 1)


def openstates_update():
    legislators = api_requests.get_legislator_data_openstates()
    legislators = make_db_id.map_digit_id(legislators, "name", "legislator_id")
    return legislators


def committee_update():
    committees = committee_scraper.get_assembly_cmte_urls()
    committees = make_db_id.map_digit_id(committees, "name", "committee_id")
    return committees


def get_foreign_key(foreign_array, field, val):
    for e in foreign_array:
        if e[field] == val:
            return e
        else:
            continue


def map_committee_memberships(legislators, committees):
    results = list()
    for committee in committees:
        url = committee["webpage_link"]
        cmte_id = committee["committee_id"]
        memberships = committee_scraper.get_assembly_cmte_members(url)
        for member in memberships:
            try:
                member["legislator_id"] = get_foreign_key(legislators, "name", member["name"])["legislator_id"]
            except:
                print(member["name"], committee)
            del member["name"]
            member["committee_id"] = cmte_id
        results.extend(memberships)
    results = make_db_id.generate_digit_id(results, "committee_assignment_id")
    return results


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


def insert_committees(cur, conn, committees):
    for committee in committees:
        try:
            insert_query = """INSERT INTO ca.committee 
            (committee_id, chamber_id, name, webpage_link) 
            VALUES (%s, %s, %s, %s)"""
            committee_to_insert = (
                committee["committee_id"],
                committee["chamber_id"],
                committee["name"],
                committee["webpage_link"]
            )
            cur.execute(insert_query, committee_to_insert)
            conn.commit()
            count = cur.rowcount
            print(count, "Committee inserted successfully into committee table")
        except (Exception, psycopg2.Error) as error:
            print("Failed to insert committee into committee table", error)
            exit(1)
    return


def insert_committee_assignments(cur, conn, memberships):
    for membership in memberships:
        try:
            insert_query = """INSERT INTO ca.committee_assignment
            (committee_id, chamber_id, name, webpage_link) 
            VALUES (%s, %s, %s, %s)"""
            assignment_to_insert = (
                membership["committee_assignment_id"],
                membership["committee_id"],
                membership["legislator_id"],
                membership["assignment_type"]
            )
            cur.execute(insert_query, assignment_to_insert)
            conn.commit()
            count = cur.rowcount
            print(count, "Assignment inserted successfully into table")
        except (Exception, psycopg2.Error) as error:
            print("Failed to insert assignment into table", error)
            exit(1)
    return


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
        # cur.execute("DELETE FROM ca.legislator")
        # cur.execute("DELETE FROM ca.committee")
        cur.execute("DELETE FROM ca.committee_assignment")
        # insert legislators into table
        legislators = openstates_update()
        # insert_legislators(cur, conn, legislators)
        # insert committees into table
        committees = committee_update()
        # insert_committees(cur, conn, committees)
        # map legislators to committees, insert assignments into table
        assignments = map_committee_memberships(legislators, committees)
        for a in assignments:
            print(a)
        insert_committee_assignments(cur, conn, assignments)
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
