"""
Parameters and functions that directly fetch from Openstates via GET requests.

Called in session_update.py
"""

import json
from config import config
from time import sleep
import requests


# Global constants

ENDPOINTS = {
    "people": "https://v3.openstates.org/people",
    "committees": "https://v3.openstates.org/committees",
}
WAIT_TIME = 10  # openstates has a rate limit of 6 requests/minute
BASE_PARAMS = { # people schema does not use session param
    "jurisdiction": "California",
    "sort": "updated_asc",  # only usable option, unfortunately this could lead to skipped rows if updates happen during sync
    "per_page": 20,  # max allowed by openstates
    "apikey": config("openstates")["api_key"],
}
ASSEMBLY_PARAMS = {
    "org_classification": "lower",
    "include": ["other_names", "links", "offices", "sources"],
}
SENATE_PARAMS = {
    "org_classification": "upper",
    "include": ["other_names", "links", "offices", "sources"],
}


def process_legislator_json(data, last_update):
    """
    Input: JSON data, update timestamp
    Output: dictionary of strings mapped to nested lists
    """
    people = [] # core person data
    people_roles = [] # correpsonding role and metadata
    people_offices = [] # corresponding office metadata
    people_names = [] # corresponding alternate names
    people_sources = [] # corresponding primary sources

    for next_person in data:
        if next_person["updated_at"] == last_update:
            continue

        # process people data
        person = []
        person.append(next_person["id"])
        person.append(next_person["name"])
        person.append(next_person["party"])
        person.append(next_person["updated_at"])
        # Update list
        people.append(person)

        # process people roles
        role = []
        role.append(next_person["id"])
        role.append(next_person["current_role"]["org_classification"])
        role.append(next_person["current_role"]["district"])
        # Update list
        people_roles.append(role)

        # process office data
        for next_office in next_person["offices"]: 
            office = []
            # Openstates people ID
            office.append(next_person["id"])
            # Official name
            office.append(next_office["name"])
            # Phone number
            office.append(next_office["voice"])
            # Street address and room
            office.append(next_office["address"])
            # classification
            office.append(next_office["classification"])
            # Update list
            people_offices.append(office)

        # process name data
        for next_name in next_person["other_names"]:
            name = []
            # Openstates people ID
            name.append(next_person["id"])
            # Alternate name
            name.append(next_name["name"])
            # Update list
            people_names.append(name)
        
        for next_source in next_person["sources"]:
            source = []
            # Openstates people ID
            source.append(next_person["id"])
            # Info source URL
            source.append(next_source["url"])
            # Update list
            people_sources.append(source)

    return {
        "people": people, 
        "people_roles": people_roles,
        "people_offices": people_offices,
        "people_names": people_names,
        "people_sources": people_sources
    }


def fetch_assembly_batch(page, updated_since):
    """
    Input: page number, timestamp
    Output: JSON API response, max page number

    Update API request parameters with page number and timestamp value (optional), and execute GET request
    """
    # Sleep to avoid timeout errors
    sleep(WAIT_TIME)

    params = {**BASE_PARAMS, **ASSEMBLY_PARAMS}
    params["page"] = page

    if updated_since != None:
        params["updated_since"] = updated_since

    result = requests.get(url=ENDPOINTS["people"], params=params).json()
    return result["results"], result["pagination"]["max_page"]


def fetch_senate_batch(page, updated_since):
    """
    Input: page number, timestamp
    Output: JSON API response, max page number

    Update API request parameters with page number and timestamp value (optional), and execute GET request
    """
    # Sleep to avoid timeout errors
    sleep(WAIT_TIME)

    params = {**BASE_PARAMS, **SENATE_PARAMS}
    params["page"] = page

    if updated_since != None:
        params["updated_since"] = updated_since

    result = requests.get(url=ENDPOINTS["people"], params=params).json()
    return result["results"], result["pagination"]["max_page"]


def get_assembly_data(page=1, updated_since=None):
    """
    Input: page number, timestamp
    Output: dictionary of string keys mapped to lists of data, max page number value

    Fetches data batch for a response page, parses into Python lists, and a max page value for downstream logic
    """

    # Fetch data for a specified API response page
    assembly_data, num_pages = fetch_assembly_batch(page, updated_since)
    # Return JSON of processed assembly members
    return process_legislator_json(assembly_data, updated_since), num_pages


def get_senate_data(page=1, updated_since=None):
    senate_data, num_pages = fetch_senate_batch(page, updated_since)
    return process_legislator_json(senate_data, updated_since), num_pages


def main():
    get_senate_data()
    return


if __name__ == "__main__":
    main()
