from flask import Flask
import urllib.request, json
from datetime import date
from config import config
from time import sleep
import sys

app = Flask(__name__)
app.config['api_key'] = config('credentials.ini', 'openstates')

current_date = date.today().strftime("%Y-%m-%d")
current_year = int(date.today().strftime("%d/%m/%Y")[-4:])
next_year = current_year + 1
current_session = str(current_year) + str(next_year)
api_startpage_string = "&page=1"
daily_max = 10


@app.route('/bill-data-openstates')
def get_bill_votes_data_openstates():
    url_source = "https://v3.openstates.org/bills?"
    jurisdiction_session_filter = "jurisdiction=California&session=20232024"
    # sort_string = f"&sort=updated_desc&updated_since={current_date}"
    sort_string = "&sort=updated_desc&updated_since=2023-01-01"
    include_string = "&include=sponsorships&include=abstracts&include=votes"
    api_key_string = "&apikey=" + app.config['api_key']['api_key']
    url = url_source + jurisdiction_session_filter + sort_string + include_string + api_startpage_string + api_key_string
    response = urllib.request.urlopen(url)
    data = response.read()
    data_dict = json.loads(data)
    max_page = data_dict["pagination"]["max_page"]
    print(f"there are {max_page} pages")
    max_page = min(daily_max, int(max_page))
    results_array = data_dict["results"]
    for i in range(2, max_page + 1):
        api_page_string = f"&page={i}"
        curr = url_source + jurisdiction_session_filter + sort_string + include_string + api_page_string + api_key_string
        print(f"requesting page {i}")
        response = urllib.request.urlopen(curr)
        data = response.read()
        sleep(1)
        data_dict = json.loads(data)
        results_array.extend(data_dict["results"])

    # Create an array of dicts that holds all data about bills
    bills = list()
    house_votes = list()
    for obj in results_array:
        if obj["session"] == current_session:
            author = ""
            for sponsor in obj["sponsorships"]:
                if sponsor["primary"]:
                    author = sponsor["name"]
            bill_text = ""
            if len(obj["abstracts"]) > 0:
                bill_text = obj["abstracts"][0]["abstract"]
            else:
                bill_text = ""
            bill_data = {
                "name": obj["title"],
                "bill_num": obj["identifier"],
                "bill_text": bill_text,
                "origin_house_id": obj["from_organization"]["name"],
                "author": author
            }
            bills.append(bill_data)

        bill_num_var = obj["identifier"]
        votes_array = obj["counts"]
        yes = 0
        no = 0
        for vote in votes_array:
            if vote["option"] == "yes":
                yes += vote["value"]
            else:
                no += 1
            house_vote_result_data = {
                "bill_num": bill_num_var,
                "date": obj["votes"][0]["start_date"],
                "votes_for": yes,
                "votes_against": no
            }
            house_votes.append(house_vote_result_data)

    return bills, house_votes


def main():
    get_bill_votes_data_openstates()


if __name__ == "__main__":
    main()
