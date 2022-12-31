from flask import Flask
import urllib.request, json

app = Flask(__name__)

@app.route('/openstates')
def get_bills():
	url = "https://v3.openstates.org/bills?jurisdiction=California&sort=updated_desc&page=1&per_page=10&apikey="

	response = urllib.request.urlopen(url)
	data = response.read()
	data_dict = json.loads(data)

	# Find the bill identifier (like "SB 1162") and bill name (like "Upzoning in high resource areas");
	# put them in a dictionary where the key = identifier, value = dictionary that so far just has the 
	# bill name in it
	results_dict = {}
	for bill in data_dict["results"]:
		# Use the 2021-2022 session 
		if bill["session"] == "20212022":
			results_dict.update({
				bill["identifier"]: 
					{"bill_name": bill["title"]}
				})

	return results_dict


# jurisdiction needs to be an id or a name:
# "id": "ocd-jurisdiction/country:us/state:ca/government",
# "name": "California"