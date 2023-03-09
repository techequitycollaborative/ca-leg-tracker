from flask import Flask
import urllib.request, json
import psycopg2
from psycopg2 import Error
from flask import jsonify
from bs4 import BeautifulSoup as bs
from dateutil import parser
import datetime
import urllib.request
from random import randint

app = Flask(__name__)

# TODO(juliacordero): Use pagination data to make sure we're getting all data.
@app.route('/bill-data-openstates')
def get_bill_data_openstates():
	url = "https://v3.openstates.org/bills?jurisdiction=California&sort=updated_desc&include=sponsorships&page=1&per_page=10&apikey="

	response = urllib.request.urlopen(url)
	data = response.read()
	data_dict = json.loads(data)

	# Create an array of dicts that holds all data about bills
	results_array = []
	for obj in data_dict["results"]:
		if obj["session"] == "20232024":
			author = ""
			for sponsor in obj["sponsorships"]:
				if sponsor["primary"]: author = sponsor["name"]
			bill_data = {
				"name": obj["title"],
				"bill_num": obj["identifier"],
				"origin_house_id": obj["from_organization"]["name"],
				"author": author
			}
			results_array.append(bill_data)

# TODO(juliacordero): Use pagination data to make sure we're getting all data.
@app.route('/committee-data-openstates')
def get_committee_data_openstates():
	url = "https://v3.openstates.org/committees?jurisdiction=CA&classification=committee&include=links&apikey={INSERTKEY}&page=1&per_page=20"
	response = urllib.request.urlopen(url)
	data = response.read()
	data_dict = json.loads(data)

	# Create an array of dicts that holds all data about cmtes
	results_array = []
	for obj in data_dict["results"]:
		homepage_link = ""
		for curr_link in obj["links"]:
				if curr_link["note"] == "homepage": homepage_link = curr_link["url"]
		cmte_data = {
			name: obj["name"],
			webpage_link: homepage_link
		}
		results_array.append(cmte_data)

# TODO(juliacordero): house_vote_result
@app.route('/house-vote-result-data-openstates')
def get_house_vote_result_data_openstates():
	url = "https://v3.openstates.org/bills?jurisdiction=CA&classification=committee&include=links&apikey={INSERTKEY}&page=1&per_page=20"

# jurisdiction needs to be an id or a name:
# "id": "ocd-jurisdiction/country:us/state:ca/government",
# "name": "California"

### leginfo scraping ###
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