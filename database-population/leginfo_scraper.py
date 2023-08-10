from bs4 import BeautifulSoup as bs
from dateutil import parser
from datetime import date
import urllib.request
import logging


def make_soup(page, tag_pattern):  # HELPER FUNCTION
    url = urllib.request.urlopen(page).read()
    soup = bs(url, "html.parser")
    return soup.select(tag_pattern)


def get_query(bill_number, session_year):  # HELPER FUNCTION
    bill_number = bill_number.replace("-", "").replace(" ", "")
    return f"{session_year}0{bill_number}"


def text_to_date_string(s):
    try:
        dt = parser.parse(s)
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass


def bill_number_history(bill_number: str, session_year="20232024"):
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
        # logging.debug(f"table row {i} detected, chronological order {chronological_order}.")
        chronological_order += 1
        date = history_soup[i].select("td")[0].text
        action = history_soup[i].select("td")[1].text
        # logging.info(f"date detected: {date}")
        # logging.info(f"action recorded: {action}")
        record = {
            "date": text_to_date_string(date),
            "action": action
        }
        action_records.append(record)
    return action_records


def bill_number_text(bill_number: str, session_year="20232024"):
    bill_query = get_query(bill_number, session_year)
    page = "https://leginfo.legislature.ca.gov/faces/billTextClient.xhtml?bill_id=" + bill_query
    text_tag = "div[id='bill_all']"
    text_soup = make_soup(page, text_tag)[0]
    return text_soup  # TODO: confirm the desired data type for bill text
