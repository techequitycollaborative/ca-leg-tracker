import psycopg2
from config import config
from bs4 import BeautifulSoup as bs
from dateutil import parser
import datetime
import urllib.request
import logging
from random import randint


logging.basicConfig(
    filename='log',
    level=logging.DEBUG,
    encoding='utf-8',
    format='%(levelname)s:%(asctime)s:%(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)


def make_soup(page: str, tag_pattern: str):  # HELPER FUNCTION
    url = urllib.request.urlopen(page).read()
    soup = bs(url, "html.parser")
    return soup.select(tag_pattern)


def get_query(bill_number: str, session_year: str) -> str:  # HELPER FUNCTION
    bill_number = bill_number.replace("-", "")
    return f"{session_year}0{bill_number}"


def text_to_date_string(s: str) -> datetime.datetime:
    try:
        dt = parser.parse(s)
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass


def bill_number_history(bill_number: str, sql_bill_id: int, session_year="20232024"):
    """
    Iterates over the entire history of actions related to bill BILL_NUMBER from session SESSION_YEAR according to the
    bill's history page and prints current status to terminal.
    """
    bill_query = get_query(bill_number, session_year)
    page = "https://leginfo.legislature.ca.gov/faces/billHistoryClient.xhtml?bill_id=" + bill_query
    history_table_tag_pattern = "table[id='billhistory'] > tbody > tr"
    history_soup = make_soup(page, history_table_tag_pattern)
    action_records = []
    chronological_order = 1
    for i in reversed(range(len(history_soup))):
        logging.debug(f"table row {i} detected, chronological order {chronological_order}.")
        chronological_order += 1
        date = history_soup[i].select("td")[0].text
        action = history_soup[i].select("td")[1].text
        logging.info(f"date detected: {date}")
        logging.info(f"action recorded: {action}")
        action_records.append((randint(0, 500), sql_bill_id, text_to_date_string(date), action))
    return action_records


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
