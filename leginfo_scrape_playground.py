from bs4 import BeautifulSoup as bs
import urllib.request
import logging

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


def get_id(bill_number: str, session_year: str) -> str:  # HELPER FUNCTION
    return f"{session_year}0{bill_number}"


def actions_from_history_table(page: str):
    """
    Iterate over rows in the table containing the last 5 actions taken on a bill, found on a provided bill page.
    """
    history = make_soup(page, "table[id='billhistory'] > tbody > tr")
    for i in range(len(history)):
        logging.info(f"distinct table row {i} detected.")
        date = history[i].select("td[scope='row']")[0]
        action = history[i].select("td")[1]
        logging.info(f"date detected: {date.text}")
        logging.info(f"action recorded: {action.text}")


def add_param_to_search(search_query: str, param: str, value: str) -> str:
    """
    Adds parameter PARAM to a query string with value VALUE.
    """
    if len(value):
        search_query += f"{param}={value}"
    return search_query


def actions_from_search(
        bill_number: str,
        session_year="20232024",
        author="All",
        law_code="All",
        code_section="",
        keyword=""
):
    """
    [WIP] Selects a bill page from a search query and iterates over the last 5 actions taken from the bill's status
    page.
    """
    search_query = f'session_year={session_year}&bill_number={bill_number}&house=Both&author={author}&lawCode={law_code}'
    add_param_to_search(search_query, "keyword", keyword)
    add_param_to_search(search_query, "lawSectionNum", code_section)
    page = "https://leginfo.legislature.ca.gov/faces//billSearchClient.xhtml?" + search_query
    # TODO: auto-redirect situation
    # TODO: multiple search matches...
    return


def actions_from_bill_number_status(bill_number: str, session_year="20232024"):
    """
    Iterates over the last 5 actions taken from the status page of a bill BILL_NUMBER from session SESSION_YEAR.
    """
    bill_id = get_id(bill_number, session_year)
    history_table_tag_pattern = "table[id='billhistory'] > tbody > tr"
    page = "https://leginfo.legislature.ca.gov/faces/billStatusClient.xhtml?bill_id=" + bill_id
    logging.debug(f"Making soup from {page}")
    history = make_soup(page, history_table_tag_pattern)
    for i in reversed(range(len(history))):
        logging.debug(f"table row {i} detected.")
        date = history[i].select("td[scope='row']")[0]
        action = history[i].select("td")[1]
        logging.info(f"date detected: {date.text}")
        logging.info(f"action recorded: {action.text}")


def bill_number_history(bill_number: str, session_year="20232024"):
    """
    Iterates over the entire history of actions related to bill BILL_NUMBER from session SESSION_YEAR according to the
    bill's history page.
    """
    bill_id = get_id(bill_number, session_year)
    page = "https://leginfo.legislature.ca.gov/faces/billHistoryClient.xhtml?bill_id=" + bill_id
    history_table_tag_pattern = "table[id='billhistory'] > tbody > tr"
    history = make_soup(page, history_table_tag_pattern)
    chronological_order = 1
    for i in reversed(range(len(history))):
        logging.debug(f"table row {i} detected, chronological order {chronological_order}.")
        chronological_order += 1
        date = history[i].select("td")[0]
        action = history[i].select("td")[1]
        logging.info(f"date detected: {date.text}")
        logging.info(f"action recorded: {action.text}")


def main():
    logging.info("Beginning scrape session...")
    bill_number_history("AB1162", "20212022")


if __name__ == "__main__":
    main()
