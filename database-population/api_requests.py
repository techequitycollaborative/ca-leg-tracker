from flask import Flask
import urllib.request
import urllib.error
import json
import datetime
from config import config
from time import sleep
import sys
from random import uniform
import logging

logging.basicConfig(
    filename="openstates.log",
    encoding='utf-8',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)

app = Flask(__name__)
app.config['api_key'] = config('openstates')
current_date = datetime.date.today().strftime("%Y-%m-%d")
current_year = int(datetime.date.today().strftime("%d/%m/%Y")[-4:])
next_year = current_year + 1
# current_session = str(current_year) + str(next_year)
current_session = "20232024"
start_page_string = "&page=1&per_page=10"
daily_max = 499
chamber_map = {
    "Assembly": 1,
    "Senate": 2,
    "lower": 1,
    "upper": 2
}


# Generic API request with exponential backoff
def make_api_request(api_request_string, pause=0.0, backoff=0, tries=1):
    try:
        sleep(pause)
        req = urllib.request.Request(
            api_request_string,
            headers={
                'User-Agent': 'tester 0.1'
            }
        )
        response = urllib.request.urlopen(req)
        logging.info(f"Got response for {api_request_string} in {tries} tries.")
        return json.loads(response.read())
    except urllib.error.HTTPError as error:
        logging.info(error)
        if pause <= 30:
            backoff += 1
            tries += 1
            pause = (2.5 ** backoff) - uniform(0, (pause * 0.2))
            logging.info(f"Increasing pause length to {pause}")
            make_api_request(api_request_string, pause, backoff, tries)
        else:
            print("No response from server; try again later.")
            sys.exit(0)


# TODO: confirm author conditionals compared to OS API
# Clean up results of API request for bill table - CALLED BY get_today_bill_votes and get_named_bill_votes
def extract_bill_table_data(results_array):
    bills = list()
    for obj in results_array:
        if obj["session"] == current_session:
            author = ""
            coauthors = []
            for sponsor in obj["sponsorships"]:
                if sponsor["classification"] == "author":
                    if sponsor["primary"]:
                        author = sponsor["person"]["name"]
                    else:
                        coauthors.append(sponsor["name"])
            bill_text = ""
            if len(obj["abstracts"]) > 0:
                bill_text = obj["abstracts"][0]["abstract"]
            bill_data = {
                "bill_name": obj["title"],
                "bill_number": obj["identifier"],
                "full_text": bill_text,
                "origin_chamber_id": chamber_map[obj["from_organization"]["name"]],
                "author": author,
                "coauthors": ", ".join(coauthors),
                "committee_id": 0,  # TODO: figure out committee ID population...
                "status": obj["latest_action_description"],
                "leginfo_link": obj["sources"][0]["url"],
                "leg_session": current_session

            }
            bills.append(bill_data)
    return bills


# Clean up results of API request for chamber votes
def extract_chamber_vote_result_data(results_array):
    chamber_votes = list()
    for obj in results_array:
        votes_array = obj["votes"]
        for vote_event in votes_array:
            vote_time = vote_event["start_date"]
            vote_date = datetime.datetime.strptime(vote_time.split("T")[0], "%Y-%m-%d").date()
            vote_house = chamber_map[vote_event["organization"]["classification"]]
            vote_count_map = dict()
            for vote_count in vote_event["counts"]:
                vote_count_map[vote_count["option"]] = vote_count["value"]
            chamber_vote_result_data = {
                "vote_date": vote_date,
                "chamber_id": vote_house,
                "votes_for": vote_count_map["yes"],
                "votes_against": vote_count_map["no"],
                "votes_other": vote_count_map["other"],
                "bill_number": obj["identifier"]
            }
            chamber_votes.append(chamber_vote_result_data)
    return chamber_votes


# API request for bills and associated voting events
@app.route('/bill-data-openstates')
def get_today_bill_votes():
    url_source = "https://v3.openstates.org/bills?"
    jurisdiction_session_filter = "jurisdiction=California&session=20232024"
    sort_string = f"&sort=updated_desc&created_since={current_date}&classification=bill"
    include_string = "&include=sponsorships&include=abstracts&include=actions&include=sources&include=votes"
    api_key_string = "&apikey=" + app.config['api_key']['api_key']
    url = url_source + jurisdiction_session_filter + sort_string + include_string + start_page_string \
        + api_key_string
    data_dict = make_api_request(url)
    if data_dict:
        max_page = data_dict["pagination"]["max_page"]
        print(f"there are {max_page} pages")
        max_page = min(daily_max, int(max_page))
        results_array = data_dict["results"]
        for i in range(2, max_page + 1):
            api_page_string = f"&page={i}&per_page=10"
            curr = url_source + jurisdiction_session_filter + sort_string + include_string + api_page_string \
                + api_key_string
            data_dict = make_api_request(curr)
            if data_dict:
                results_array.extend(data_dict["results"])
        # Create an array of dicts that holds all data about bills
        bills = extract_bill_table_data(results_array)
        chamber_votes = extract_chamber_vote_result_data(results_array)
        return bills, chamber_votes


@app.route('/bill-data-openstates')
def get_named_bill_votes(bill_name):
    url_source = "https://v3.openstates.org/bills?"
    jurisdiction_session_filter = "jurisdiction=California&session=20232024"
    identifier = f'&identifier={bill_name}'
    sort_string = f"&sort=updated_desc"
    include_string = "&include=sponsorships&include=abstracts&include=actions&include=sources&include=votes"
    api_key_string = "&apikey=" + app.config['api_key']['api_key']
    url = url_source + jurisdiction_session_filter + identifier + sort_string + include_string + start_page_string \
        + api_key_string
    data_dict = make_api_request(url)
    if data_dict:
        results_array = data_dict["results"]
        # Create an array of dicts that holds all data about bills
        bills = extract_bill_table_data(results_array)
        chamber_votes = extract_chamber_vote_result_data(results_array)
        return bills, chamber_votes


# API request for legislators
@app.route('/legislator-data-openstates')
def get_legislator_data_openstates():
    url_source = "https://v3.openstates.org/people?"
    jurisdiction_session_filter = "jurisdiction=California"
    sort_string = "&org_classification="
    include_string = "&include=links"
    api_key_string = "&apikey=" + app.config['api_key']['api_key']
    chambers = {
        "lower": None,
        "upper": None
    }
    for chamber in chambers.keys():
        curr_sort = sort_string + chamber
        url = url_source + jurisdiction_session_filter + curr_sort + include_string + start_page_string \
            + api_key_string
        data_dict = make_api_request(url)
        max_page = data_dict["pagination"]["max_page"]
        print(f"there are {max_page} pages")
        max_page = min(daily_max, int(max_page))
        results_array = data_dict["results"]
        for i in range(2, max_page + 1):
            api_page_string = f"&page={i}&per_page=50"
            curr = url_source + jurisdiction_session_filter + curr_sort + include_string + api_page_string \
                + api_key_string
            data_dict = make_api_request(curr)
            if data_dict:
                results_array.extend(data_dict["results"])
        chambers[chamber] = extract_legislator_table_data(results_array, chamber)

    return chambers["lower"] + chambers["upper"]


def extract_legislator_table_data(results_array, chamber_classification):
    legislators = list()
    classification_map = {
        "lower": 1,
        "upper": 2
    }
    for obj in results_array:
        chamber_id = classification_map[chamber_classification]
        legislator_data = {
            "chamber_id": chamber_id,
            "name": obj["name"],
            "district": obj["current_role"]["district"],
            "party": obj["party"],
        }
        legislators.append(legislator_data)
    return legislators


def main():
    get_named_bill_votes("AB%201")


if __name__ == "__main__":
    main()
