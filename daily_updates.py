from flask import Flask
import urllib.request, json
import psycopg2
from psycopg2 import Error
from flask import jsonify

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

	# you can push results to the database here
	# try:
	# 	# Connect to an existing database
	# 	connection = psycopg2.connect(
	# 		user="",
	# 		password="",
	# 		host="",
	# 		port="",
	# 		database="")

	# 	# Create a cursor to perform database operations
	# 	cursor = connection.cursor()
	# 	# Print PostgreSQL details
	# 	print("PostgreSQL server information\n")
	# 	print(connection.get_dsn_parameters(), "\n")
	# 	# Executing a SQL query
	# 	cursor.execute("SELECT version();")
	# 	# Fetch result
	# 	record = cursor.fetchone()
	# 	print("You are connected to - ", record, "\n")

	# except (Exception, Error) as error:
	# 	print("Error while connecting to PostgreSQL", error)
	# finally:
	# 	if (connection):
	# 		cursor.close()
	# 		connection.close()
	# 		print("PostgreSQL connection is closed")

	# return jsonify({"success": True}), 200

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