from flask import Flask
import urllib.request, json
import psycopg2
from psycopg2 import Error
from flask import jsonify
from config import config
from bs4 import BeautifulSoup as bs
from dateutil import parser
import datetime
from datetime import date
import urllib.request
from random import randint
import logging

app = Flask(__name__)
app.config['api_key'] = config('credentials', 'openstates')

current_year = int(date.today().strftime("%d/%m/%Y")[-4:])
next_year = current_year + 1
current_session = str(current_year) + str(next_year)

# open states fetching #

@app.route('/bill-data-openstates')
def get_bill_data_openstates():
	i = 1
	data_dict = {}

	# Get first page
	url = "https://v3.openstates.org/bills?jurisdiction=California&sort=updated_desc&include=sponsorships&include=abstracts&page=1&per_page=10&apikey=" + app.config['api_key']['api_key']

	response = urllib.request.urlopen(url)
	data = response.read()
	data_dict = json.loads(data)
	results_array = data_dict["results"]

	max_page = data_dict["pagination"]["max_page"]

	for i in range (2, max_page):
		url = "https://v3.openstates.org/bills?jurisdiction=California&sort=updated_desc&include=sponsorships&include=abstracts&page=" + str(i) + "&per_page=10&apikey=" + app.config['api_key']['api_key']
		response = urllib.request.urlopen(url)
		data = response.read()
		data_dict = json.loads(data)
		results_array.extend(data_dict["results"])

	# Create an array of dicts that holds all data about bills
	formatted_results_array = []
	for obj in results_array:
		if obj["session"] == current_session:
			author = ""
			for sponsor in obj["sponsorships"]:
				if sponsor["primary"]: author = sponsor["name"]
			bill_data = {
				"name": obj["title"],
				"bill_text": obj["abstracts"][0]["abstract"],
				"bill_num": obj["identifier"],
				"origin_house_id": obj["from_organization"]["name"],
				"author": author
			}
			formatted_results_array.append(bill_data)
	return formatted_results_array

@app.route('/committee-data-openstates')
def get_committee_data_openstates():
	i = 1
	data_dict = {}

	url = "https://v3.openstates.org/committees?jurisdiction=CA&classification=committee&include=links&page=1&per_page=20&apikey={INSERTKEY}"
	response = urllib.request.urlopen(url)
	data = response.read()
	data_dict = json.loads(data)
	results_array = data_dict["results"]

	max_page = data_dict["pagination"]["max_page"]

	for i in range (2, max_page):
		url = "https://v3.openstates.org/committees?jurisdiction=CA&classification=committee&include=links&page=" + str(i) + "&per_page=20&apikey={INSERTKEY}"
		response = urllib.request.urlopen(url)
		data = response.read()
		data_dict = json.loads(data)
		results_array.extend(data_dict["results"])

	# Create an array of dicts that holds all data about cmtes
	formatted_results_array = []
	for obj in results_array:
		homepage_link = ""
		for curr_link in obj["links"]:
			if curr_link["note"] == "homepage": homepage_link = curr_link["url"]
		cmte_data = {
			name: obj["name"],
			webpage_link: homepage_link
		}
		formatted_results_array.append(cmte_data)
	return formatted_results_array

@app.route('/house-vote-result-data-openstates')
def get_house_vote_result_data_openstates():
	i = 1
	data_dict = {}

	url = "https://v3.openstates.org/bills?jurisdiction=California&sort=updated_desc&include=votes&page=1&per_page=20&apikey={INSERTKEY}"
	response = urllib.request.urlopen(url)
	data = response.read()
	data_dict = json.loads(data)
	results_array = data_dict["results"]

	max_page = data_dict["pagination"]["max_page"]

	for i in range (2, max_page):
		url = "https://v3.openstates.org/bills?jurisdiction=California&sort=updated_desc&include=votes&page=" + str(i) + "&per_page=20&apikey={INSERTKEY}"
		response = urllib.request.urlopen(url)
		data = response.read()
		data_dict = json.loads(data)
		results_array.extend(data_dict["results"])

	formatted_results_array = []
	for obj in results_array:
		bill_num_var = obj["identifier"]

		votes_array = obj["votes"]["votes"]

		yes = 0
		no = 0
		for vote in votes_array:
			if vote["option"] == "yes": yes += 1
			else: no += 1 
		house_vote_result_data = {
			bill_num: bill_num_var,
			date: obj["votes"]["start_date"],
			votes_for: yes,
			votes_against: no
		}
		formatted_results_array.append(house_vote_result_data)

	return formatted_results_array


# leginfo scraping #


def make_soup(page, tag_pattern):  # HELPER FUNCTION
	url = urllib.request.urlopen(page).read()
	soup = bs(url, "html.parser")
	return soup.select(tag_pattern)


def get_query(bill_number, session_year):  # HELPER FUNCTION
	bill_number = bill_number.replace("-", "")
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
		logging.debug(f"table row {i} detected, chronological order {chronological_order}.")
		chronological_order += 1
		date = history_soup[i].select("td")[0].text
		action = history_soup[i].select("td")[1].text
		logging.info(f"date detected: {date}")
		logging.info(f"action recorded: {action}")
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
