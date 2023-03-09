import psycopg2
from config import config
import daily_updates
import logging
import datetime

logging.basicConfig(
    filename='log',
    level=logging.DEBUG,
    encoding='utf-8',
    format='%(levelname)s:%(asctime)s:%(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)


def pull_daily_update():
    # all active bills - extracted from OpenStates as an array
    today_bills = daily_updates.get_bill_data_openstates()
    # most updated bill histories - extracted from LegInfo
    year = datetime.date.today().strftime("%Y")
    session_year = year + str(int(year) + 1)
    print(session_year)
    for bill in today_bills:
        bill_number = bill["bill_num"]
        today_bill_records = daily_updates.bill_number_history(bill_number, session_year)
        bill["status"] = today_bill_records[-1]
        bill["text"] = daily_updates.bill_number_text(bill_number, session_year)
        bill["actions_update"] = today_bill_records  # TODO: push to bill_history table only
    # most updated committee vote results
    today_cmte_vote_result = daily_updates.get_committee_data_openstates()
    # most updated house vote results
    today_house_vote_result = daily_updates.get_house_vote_result_data_openstates()
    return today_bills, today_cmte_vote_result, today_house_vote_result


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config("postgresql")

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        bills, committee_results, house_results = pull_daily_update()
        # TODO: insert today's bills to the bill table and bill_history table
        # TODO: insert committee results to committee results table
        # TODO: insert house results to house results table

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
