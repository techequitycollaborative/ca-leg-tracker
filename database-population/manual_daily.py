import argparse
import api_requests
import leginfo_scraper
import psycopg2
from config import config
SESSION_YEAR = "20232024"


def full_openstates_update():
    return api_requests.get_today_bill_votes()


def requested_openstates_update():
    bills = []
    chamber_votes = []
    with open("bill_request_list.txt", mode="r") as f:
        for curr_bill in f:
            curr_bill = "%20".join(curr_bill.strip().split(" "))
            results = api_requests.get_named_bill_votes(curr_bill)
            bills.extend(results[0])
            chamber_votes.extend(results[1])
    return bills, chamber_votes


def leginfo_actions_update(bill_number, session_year=SESSION_YEAR):
    return leginfo_scraper.bill_number_history(bill_number, session_year)


def insert_bills(cur, conn, bills):
    for bill in bills:
        try:
            insert_query = """INSERT INTO ca_test.bill 
            (bill_name, bill_number, full_text, author, origin_chamber_id, committee_id, status, session) 
            VALUES (%s, %s, %s, %s, %s::integer, %s::integer, %s, %s)"""
            name = bill["name"]
            bill_num = bill["bill_num"]
            full_text = bill["bill_text"]
            author = bill["author"]
            origin_chamber_id = 0
            committee_id = 0
            status = ""
            session = ""
            bill_to_insert = (
                name, bill_num, full_text, author, origin_chamber_id, committee_id, status, session
            )
            cur.execute(insert_query, bill_to_insert)
            conn.commit()
            count = cur.rowcount
            print(count, "Bill inserted successfully into bill table")
        except (Exception, psycopg2.Error) as error:
            print("Failed to insert bill into bill table", error)


def insert_chamber_votes(cur, conn, votes):
    insert_query = """INSERT into ca_test.house_vote_result 
                (vote_date, bill_id, chamber_id, votes_for, votes_against) 
                VALUES (%s, %s::integer, %s::integer, %s::integer, %s::integer)
                """
    for bill_id, session_year in cur.fetchall():
        cur.execute(
            'SELECT bill_id, bill_number FROM ca_test.bill WHERE bill_id = (SELECT bill_id FROM ca_test.bill WHERE '
            'bill_id = {bill_id});'
        )
        bill_id = cur.fetchone()[0]
        bill_number = cur.fetchone()[1]
        for vote in votes:
            try:
                vote_date = vote["date"]
                house_id = vote["house_id"]
                votes_for = vote["votes_for"]
                votes_against = vote["votes_against"]
                vote_to_insert = (vote_date, bill_id, house_id, votes_for, votes_against)
                cur.execute(insert_query, vote_to_insert)
                conn.commit()
                count = cur.rowcount
                print(count, "Vote event inserted successfully into table")
            except (Exception, psycopg2.Error) as error:
                print("Failed to insert vote event in vote table", error)
        conn.commit()
        cur.close()
        return


def update_bill_history(cur, conn):
    cur.execute('SELECT bill_id, session FROM ca_test.bill;')
    # load all actions from leginfo for each bill ID
    for bill_id, session_year in cur.fetchall():
        cur.execute(
            'SELECT bill_number FROM ca.bill WHERE bill_id = (SELECT bill_id FROM ca.bill WHERE bill_id = {bill_id});')
        bill_number = cur.fetchone()[0]
        actions_for_bill = leginfo_actions_update(bill_number, session_year)
        # dump all actions to bill_history
        for action in actions_for_bill:
            print(action)
            insert_script = """
            INSERT INTO ca.bill_history (bill_id, entry_date, entry_text) 
            VALUES (%s::integer, %s::date, %s);"""
            cur.execute(insert_script, action)
            conn.commit()
    # close the communication with the PostgreSQL
    conn.commit()
    cur.close()
    return


def connect(args):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config("postgresql")

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()
        # generate update

        updated_bills = []
        updated_chamber_votes = []
        if args.daily:
            updated_bills, updated_chamber_votes = full_openstates_update()
        if args.named:
            updated_bills, updated_chamber_votes = requested_openstates_update()
        # insert today's bills to the bill table
        insert_bills(cur, conn, updated_bills)
        # insert house updates to house results table
        insert_chamber_votes(cur, conn, updated_chamber_votes)
        # update bill history
        update_bill_history(cur, conn)

    except (Exception, psycopg2.DatabaseError) as error:
        print("Failed to update records", error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def main(arg_list):
    connect(arg_list)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Manually populate items in ca.*')
    parser.add_argument('--today', dest='daily', action='store_const', const=True,
                        default=False, help='requests info for all bills created today.')
    parser.add_argument('--named', dest='named', action='store_const', const=True,
                        default=False, help='requests info for all bills specified in `bill_request_list.txt`.')
    args = parser.parse_args()
    main(args)
