import psycopg2
from config import config
import daily_updates
import logging


logging.basicConfig(
    filename='log',
    level=logging.DEBUG,
    encoding='utf-8',
    format='%(levelname)s:%(asctime)s:%(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # manually insert bill info for testing purposes
        # bill_insert_script = 'INSERT INTO ca.bill (bill_id, name, bill_number, full_text, author, origin_house_id, ' \
        #                      'committee_id, status, session) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        # value = (randint(0, 100), 'Public health: COVID-19 testing and dispensing sites.', 'AB-269', 'foobar', 'Berman',
        #          1, randint(0, 20), '', '20232024')
        # cur.execute(bill_insert_script, value)
        # get list of bill IDs from bill table
        cur.execute('SELECT bill_id, session FROM ca.bill;')
        # load all actions from leginfo for each bill ID
        for bill_id, session_year in cur.fetchall():
            cur.execute('SELECT bill_number FROM ca.bill '
                        f'WHERE bill_id = (SELECT bill_id FROM ca.bill WHERE bill_id = {bill_id});')
            bill_number = cur.fetchone()[0]
            actions_for_bill = bill_number_history(bill_number, bill_id, session_year)
            # dump all actions to bill_history
            for action in actions_for_bill:
                print(action)
                insert_script = 'INSERT INTO ca.bill_history ' \
                                f'(bill_history_id, bill_id, entry_date, entry_text) ' \
                                f'VALUES (%s, %s::integer, %s::date, %s);'
                cur.execute(insert_script, action)
                conn.commit()
        # close the communication with the PostgreSQL
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    connect()
