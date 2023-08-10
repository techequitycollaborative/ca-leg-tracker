import api_requests
import leginfo_scraper
import datetime
import config
import psycopg2
from random import randint

year = datetime.date.today().strftime("%Y")
SESSION_YEAR = year + str(int(year) + 1)


def add_bill_id(arr, bill_mapping):
    for bill in arr:
        bill_num = arr["identifier"]
        bill["bill_id"] = bill_mapping[bill_num]
    return arr


def openstates_update():
    updated_bills, updated_house_votes = api_requests.get_bill_votes_data_openstates()
    bill_numbers = [bill["identifier"] for bill in updated_bills]
    bill_mapping = dict([(bill_num, randint(0, max(len(updated_bills)))) for bill_num in bill_numbers])
    updated_bills = add_bill_id(updated_bills, bill_mapping)
    updated_house_votes = add_bill_id(updated_house_votes, bill_mapping)
    return updated_bills, updated_house_votes


def leginfo_actions_update(bill_number, bill_id, session_year=SESSION_YEAR):
    return leginfo_scraper.bill_number_history(bill_number, bill_id, session_year)


def insert_bills(cur, conn, bills):
    for bill in bills:
        try:
            insert_query = """INSERT INTO ca.bill 
            (bill_id, name, bill_number, full_text, author, origin_house_id, committee_id, status, session) 
            VALUES (%d, %s, %s, %s, %s, %d, %d, %s, %s)"""
            bill_id = bill["bill_id"]
            name = bill["title"]
            bill_num = bill["identifier"]
            full_text = bill["bill_text"]
            author = bill["author"]
            origin_house_id = 0
            committee_id = 0
            status = ""
            session = ""
            bill_to_insert = (bill_id, name, bill_num, full_text, author, origin_house_id, committee_id, status, session)
            cur.execute(insert_query, bill_to_insert)
            conn.commit()
            count = cur.rowcount
            print(count, "Bill inserted successfully into bill table")
        except (Exception, psycopg2.Error) as error:
            print("Failed to insert bill into bill table", error)


def insert_house_votes(cur, conn, votes):
    return


def update_bill_history(cur, conn):
    cur.execute('SELECT bill_id, session FROM ca.bill;')
    # load all actions from leginfo for each bill ID
    for bill_id, session_year in cur.fetchall():
        cur.execute(
            'SELECT bill_number FROM ca.bill WHERE bill_id = (SELECT bill_id FROM ca.bill WHERE bill_id = {bill_id});')
        bill_number = cur.fetchone()[0]
        actions_for_bill = leginfo_actions_update(bill_number, bill_id, session_year)
        # dump all actions to bill_history
        for action in actions_for_bill:
            print(action)
            insert_script = 'INSERT INTO ca.bill_history (bill_history_id, bill_id, entry_date, entry_text) VALUES (%s, %s::integer, %s::date, %s);'
            cur.execute(insert_script, action)
            conn.commit()
    # close the communication with the PostgreSQL
    conn.commit()
    cur.close()
    return


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config("credentials.ini", "postgresql")

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()
        updated_bills, updated_house_votes = openstates_update()
        # TODO: insert today's bills to the bill table
        insert_bills(cur, conn, updated_bills)
        # TODO: insert house updates to house results table
        insert_house_votes(cur, conn, updated_house_votes)
        # update bill history
        update_bill_history(cur, conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Failed to update records", error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def main():
    updated_bills, updated_house_votes = openstates_update()
    print(len(updated_bills))
    print(len(updated_house_votes))
    # connect()


if __name__ == "__main__":
    main()

