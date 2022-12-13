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


def make_soup(page, tag_pattern):
    url = urllib.request.urlopen(page).read()
    soup = bs(url, "html.parser")
    return soup.select(tag_pattern)


def actions_from_history(page):
    history = make_soup(page, "table[id='billhistory'] > tbody > tr")
    for i in range(len(history)):
        logging.info(f"distinct table row {i} detected.")
        date = history[i].select("td[scope='row']")[0]
        action = history[i].select("td")[1]
        logging.info(f"date detected: {date.text}")
        logging.info(f"action recorded: {action.text}")


def add_param_to_search(search_query, param, value):
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
    search_query = f"session_year={session_year}&bill_number={bill_number}&house=Both&author={author}&lawCode={law_code}"
    add_param_to_search(search_query, "keyword", keyword)
    add_param_to_search(search_query, "lawSectionNum", code_section)
    page = "https://leginfo.legislature.ca.gov/faces//billSearchClient.xhtml?" + search_query
    # TODO: auto-redirect situation
    # TODO: multiple search matches...
    return


def actions_from_bill_number(bill_number: str, session_year="20232024"):
    id = f"{session_year}0{bill_number}"
    history_table_tag_pattern = "table[id='billhistory'] > tbody > tr"
    page = "https://leginfo.legislature.ca.gov/faces/billStatusClient.xhtml?bill_id="+id
    logging.debug(f"Making soup from {page}")
    history = make_soup(page, history_table_tag_pattern)
    for i in reversed(range(len(history))):
        logging.debug(f"table row {i} detected.")
        date = history[i].select("td[scope='row']")[0]
        action = history[i].select("td")[1]
        logging.info(f"date detected: {date.text}")
        logging.info(f"action recorded: {action.text}")


def main():
    logging.info("Beginning scrape session...")
    actions_from_bill_number("AB1162", "20212022")

if __name__ == "__main__":
    main()
