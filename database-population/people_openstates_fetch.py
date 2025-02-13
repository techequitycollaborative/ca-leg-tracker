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
    'people': 'https://v3.openstates.org/people',
    'committees': 'https://v3.openstates.org/committees'
}
WAIT_TIME = 10 # openstates has a rate limit of 6 requests/minute
BASE_PARAMS = {
    'jurisdiction': 'California',
    'session': '20252026',
    'sort': 'updated_asc', # only usable option, unfortunately this could lead to skipped rows if updates happen during sync
    'per_page': 20, # max allowed by openstates
    'apikey': config('openstates')['api_key']
}
ASSEMBLY_PARAMS = {
    'org_classification': 'lower',
    'include': ['other_names','other_identifiers','links']
}
SENATE_PARAMS = {
    'org_classification': 'upper',
    'include': ['other_names','other_identifiers','links']
}

def process_legislator_json(data, last_update):
    """
    Input: JSON data, update timestamp
    Output: dictionary of strings mapped to nested lists
    """
    people = []
    people_roles = []
    for next_person in data:
        if next_person['updated_at'] == last_update:
            continue

        # process people data
        person = []
        person.append(next_person['id'])
        person.append(next_person['name'])
        person.append(next_person['party'])
        person.append(next_person['updated_at'])

        # process people roles
        role = []
        role.append(next_person['id'])
        role.append(next_person['current_role']['org_classification'])
        role.append(int(next_person['current_role']['district']))
        
        people.append(person)
        people_roles.append(role)

    return {
        'people': people,
        'people_roles': people_roles
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
    params['page'] = page

    if updated_since != None:
        params['updated_since'] = updated_since
    
    result = requests.get(url=ENDPOINTS['people'], params=params).json()
    return result['results'], result['pagination']['max_page']

def fetch_senate_batch(page, updated_since):
    """
    Input: page number, timestamp
    Output: JSON API response, max page number

    Update API request parameters with page number and timestamp value (optional), and execute GET request
    """
    # Sleep to avoid timeout errors
    sleep(WAIT_TIME)

    params = {**BASE_PARAMS, **SENATE_PARAMS}
    params['page'] = page

    if updated_since != None:
        params['updated_since'] = updated_since

    result = requests.get(url=ENDPOINTS['people'], params=params).json()
    return result['results'], result['pagination']['max_page']

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