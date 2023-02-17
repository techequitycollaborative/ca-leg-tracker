from bs4 import BeautifulSoup as bs
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
    return f"{session_year}0{bill_number}"


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
        date = history_soup[i].select("td")[0]
        action = history_soup[i].select("td")[1]
        logging.info(f"date detected: {date.text}")
        logging.info(f"action recorded: {action.text}")
        action_records.append((randint(0, 500), sql_bill_id, date, action))
    return action_records

def main():
    bill_number_history("AB-269", 51)


if __name__ == "__main__":
    main()