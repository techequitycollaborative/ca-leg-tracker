from flask import Flask
import urllib.request, json

app = Flask(__name__)

@app.route('/openstates')
def get_bills():
	url = "https://v3.openstates.org/bills?jurisdiction=California&sort=updated_desc&page=1&per_page=10&apikey=e3f95a13-7e19-4fef-96dd-72b1ca3da881"

	response = urllib.request.urlopen(url)
	data = response.read()
	dict = json.loads(data)

	return dict["results"][0]


# jurisdiction needs to be an id or a name:
# "id": "ocd-jurisdiction/country:us/state:ca/government",
# "name": "California"