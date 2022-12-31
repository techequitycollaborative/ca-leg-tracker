from flask import Flask
import urllib.request, json

app = Flask(__name__)

@app.route('/openstates')
def get_bills():
	url = "https://v3.openstates.org/bills?jurisdiction=California&sort=updated_desc&page=1&per_page=10&apikey="

	response = urllib.request.urlopen(url)
	data = response.read()
	data_dict = json.loads(data)

	# Create a dict that works for us
	# results_dict = {}
	# i = 0
	# for all in data_dict["results"]:
	# 	results_dict.append("i", i)
	# 	i++

	return data_dict


# jurisdiction needs to be an id or a name:
# "id": "ocd-jurisdiction/country:us/state:ca/government",
# "name": "California"