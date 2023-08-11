from flask import Flask
import urllib.request
import urllib.error
import json
from datetime import date
from config import config
from time import sleep
import sys
from tqdm import tqdm
from random import uniform

app = Flask(__name__)
app.config['api_key'] = config('openstates')
current_date = date.today().strftime("%Y-%m-%d")
current_year = int(date.today().strftime("%d/%m/%Y")[-4:])
next_year = current_year + 1
current_session = str(current_year) + str(next_year)
start_page_string = "&page=1"
daily_max = 499
house_map = {
    "lower": 1,
    "upper": 2
}


def make_api_request(api_request_string, pause=0, backoff=0):
    try:
        sleep(pause)
        response = urllib.request.urlopen(api_request_string)
        return response
    except urllib.error.HTTPError as error:
        print(error)
        if pause <= 16:
            backoff += 1
            pause = (1.2 ** backoff) - uniform(0, pause)
            print("increasing pause length to ", pause)
            make_api_request(api_request_string, pause, backoff)
        else:
            sys.exit(1)


def extract_bill_table_data(results_array):
    bills = list()
    for obj in results_array:
        if obj["session"] == current_session:
            author = ""
            for sponsor in obj["sponsorships"]:
                if sponsor["primary"]:
                    author = sponsor["name"]
            bill_text = ""
            if len(obj["abstracts"]) > 0:
                bill_text = obj["abstracts"][0]["abstract"]
            bill_data = {
                "name": obj["title"],
                "bill_num": obj["identifier"],
                "bill_text": bill_text,
                "origin_house_id": obj["from_organization"]["name"],
                "author": author
            }
            bills.append(bill_data)
    return bills


def extract_house_vote_result_data(results_array):
    house_votes = list()
    for obj in results_array:
        bill_num_var = obj["identifier"]
        votes_array = obj["votes"]
        for vote_event in votes_array:
            vote_date = vote_event["start_date"]
            vote_id = vote_event["id"]
            vote_house = house_map[vote_event["organization"]["classification"]]
            vote_count_map = dict()
            for vote_count in vote_event["counts"]:
                vote_count_map[vote_count["option"]] = vote_count["value"]
            house_vote_result_data = {
                "id": vote_id,
                "bill_num": bill_num_var,
                "date": vote_date,
                "house_id": vote_house,
                "votes_for": vote_count_map["yes"],
                "votes_against": vote_count_map["no"],
                "votes_other": vote_count_map["other"]
            }
            house_votes.append(house_vote_result_data)
    return house_votes


@app.route('/bill-data-openstates')
def get_bill_votes_data_openstates():
    url_source = "https://v3.openstates.org/bills?"
    jurisdiction_session_filter = "jurisdiction=California&session=20232024"
    # sort_string = f"&sort=updated_desc&updated_since={current_date}"
    sort_string = f"&sort=updated_desc&updated_since=2023-01-01&classification=bill"
    include_string = "&include=sponsorships&include=abstracts&include=votes"
    api_key_string = "&apikey=" + app.config['api_key']['api_key']
    url = url_source + jurisdiction_session_filter + sort_string + include_string + start_page_string \
        + api_key_string
    response = make_api_request(url)
    data = response.read()
    data_dict = json.loads(data)
    max_page = data_dict["pagination"]["max_page"]
    print(f"there are {max_page} pages")
    max_page = min(daily_max, int(max_page))
    results_array = data_dict["results"]
    pause = 1
    for i in tqdm(range(2, max_page + 1)):
        sleep(pause)
        api_page_string = f"&page={i}"
        curr = url_source + jurisdiction_session_filter + sort_string + include_string + api_page_string \
            + api_key_string
        response = make_api_request(curr)
        data = response.read()
        data_dict = json.loads(data)
        results_array.extend(data_dict["results"])

    # Create an array of dicts that holds all data about bills
    bills = extract_bill_table_data(results_array)
    house_votes = extract_house_vote_result_data(results_array)
    return bills, house_votes


def main():
    get_bill_votes_data_openstates()


if __name__ == "__main__":
    main()
