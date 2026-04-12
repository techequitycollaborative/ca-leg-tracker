"""
Parameters and functions that directly fetch from Openstates via GET requests.
"""

from config import config
from time import sleep
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
import logging

logger = logging.getLogger(__name__)
# Global constants

ENDPOINTS = {"bills": "https://v3.openstates.org/bills"}
WAIT_TIME = 10  # openstates has a rate limit of 6 requests/minute
BASE_PARAMS = {
    "jurisdiction": "California",
    "session": "20252026",
    "sort": "updated_asc",  # only usable option, unfortunately this could lead to skipped rows if updates happen during sync
    "per_page": 20,  # max allowed by openstates
    "apikey": config("openstates")["api_key"],
    "include": [
        "sponsorships",
        "abstracts",
        "other_titles",
        "other_identifiers",
        "actions",
        "documents",
        "versions",
        "votes",
        "related_bills",
    ],
}


def process_bill_json(data, last_update):
    """
    Input: JSON data, update timestamp
    Output: dictionary of strings mapped to nested lists
    """
    bills = []
    bill_actions = []
    bill_sponsors = []
    bill_votes = []

    for next_bill in data:
        if next_bill["updated_at"] == last_update:
            continue

        # process bill data
        bill = []
        bill.append(next_bill["id"])
        bill.append(next_bill["session"])
        bill.append(next_bill["from_organization"]["name"])
        bill.append(next_bill["identifier"])
        bill.append(next_bill["title"])
        bill.append(next_bill["created_at"])
        bill.append(next_bill["updated_at"])
        bill.append(next_bill["first_action_date"])
        bill.append(next_bill["latest_action_date"])
        current_abstract = ""
        for abstract in next_bill["abstracts"]:
            if abstract["note"] == "summary":
                current_abstract = abstract["abstract"]
            else:
                logger.info(
                    "found abstract of type "
                    + abstract["note"]
                    + " for bill "
                    + next_bill["identifier"]
                )
        bill.append(current_abstract.replace("\n", "\\n"))
        bills.append(bill)

        # process bill sponsors
        for next_sponsor in next_bill["sponsorships"]:
            if next_sponsor["entity_type"] == "person":
                sponsor = []
                sponsor.append(next_bill["id"])
                sponsor.append(next_sponsor["name"])
                if (
                    "person" in next_sponsor.keys()
                    and type(next_sponsor["person"]) == dict
                ):
                    sponsor.append(next_sponsor["person"]["name"])
                    if "current_role" in next_sponsor["person"].keys():
                        sponsor.append(next_sponsor["person"]["current_role"]["title"])
                        sponsor.append(
                            next_sponsor["person"]["current_role"]["district"]
                        )
                    else:
                        sponsor.append("")
                        sponsor.append("")
                sponsor.append(str(next_sponsor["primary"]))
                sponsor.append(next_sponsor["classification"])
                bill_sponsors.append(sponsor)
            else:
                logger.info(
                    "found sponsor of type "
                    + sponsor["entity_type"]
                    + " for bill "
                    + bill["identifier"]
                )

        # process bill actions
        for next_action in next_bill["actions"]:
            action = []
            action.append(next_bill["id"])
            action.append(next_action["organization"]["name"])
            action.append(next_action["description"])
            action.append(next_action["date"])
            action.append(str(next_action["order"]))
            bill_actions.append(action)

        # process bill votes
        for next_vote in next_bill["votes"]:
            vote = []
            vote.append(next_bill["id"])
            vote.append(next_vote["motion_text"])
            vote.append(next_vote["start_date"])
            vote.append(next_vote["organization"]["name"])
            vote.append(next_vote["result"])
            vote.append(next_vote["extras"]["threshold"])

            yes = 0
            no = 0
            other = 0
            counts = next_vote["counts"]
            for count in counts:
                if count["option"] == "yes":
                    yes = count["value"]
                if count["option"] == "no":
                    no = count["value"]
                if count["option"] == "other":
                    other = count["value"]

            vote.append(yes)
            vote.append(no)
            vote.append(other)
            bill_votes.append(vote)

    return {
        "bills": bills,
        "bill_actions": bill_actions,
        "bill_sponsors": bill_sponsors,
        "bill_votes": bill_votes,
    }


@retry(
    retry=retry_if_exception_type(
        (requests.exceptions.RequestException, ValueError, KeyError)
    ),
    wait=wait_exponential(multiplier=1, min=10, max=60),
    stop=stop_after_attempt(3),
)
def fetch_bill_batch(page, updated_since):
    """
    Input: page number, timestamp
    Output: JSON API response, max page number

    Update API request parameters with page number and timestamp value (optional), and execute GET request.
    Retries up to 3 times on network errors or malformed responses.
    """
    sleep(WAIT_TIME)

    params = BASE_PARAMS
    params["page"] = page

    if updated_since is not None:
        params["updated_since"] = updated_since

    response = requests.get(url=ENDPOINTS["bills"], params=params)
    
    # Log and raise on bad HTTP status (4xx, 5xx) before attempting .json()
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP {response.status_code} on page {page}: {response.text[:500]}")
        raise  # re-raise so tenacity can retry it

    # Now attempt JSON parsing with visibility into what went wrong
    try:
        result = response.json()
    except ValueError:
        logging.error(f"JSON decode failed on page {page}. Raw response: {response.text[:500]}")
        raise  # re-raise so tenacity retries

    return result["results"], result["pagination"]["max_page"]


def get_bill_data(page=1, updated_since=None):
    """
    Input: page number, timestamp
    Output: dictionary of string keys mapped to lists of data, max page number value

    Fetches data batch for a response page, parses into Python lists, and a max page value for downstream logic
    """

    # Fetch data for a specified API response page
    data, num_pages = fetch_bill_batch(page, updated_since)

    # Return JSON of processed bills
    return process_bill_json(data, updated_since), num_pages
