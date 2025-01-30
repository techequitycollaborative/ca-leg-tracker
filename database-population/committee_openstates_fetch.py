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
    'committees': 'https://v3.openstates.org/committees'
}
WAIT_TIME = 10 # openstates has a rate limit of 6 requests/minute
BASE_PARAMS = {
    'jurisdiction': 'California',
    'session': '20252026',
    'sort': 'updated_asc', # only usable option, unfortunately this could lead to skipped rows if updates happen during sync
    'per_page': 20, # max allowed by openstates
    'apikey': config('openstates')['api_key'],
    'include': ['memberships', 'links']
}


def process_committee_json(data, last_update):
    committees = []
    committee_memberships = []
    
    for next_committee in data:
        if next_committee['classification'] != 'committee':
            continue

        cmte = []
        cmte.append(next_committee['id'])
        cmte.append(next_committee['name'])

        for next_link in next_committee['links']:
            if next_link['note'] == 'homepage':
                cmte.append(next_link['url'])
        
        committees.append(cmte)
        
        for next_member in next_committee['memberships']:
            member = []
            member.append(next_committee['id'])
            if 'person' not in next_member.keys():
                print("NO OPENSTATES ID FOR THIS PERSON")
                print(next_member['person_name'])
            else:
                member.append(next_member['person']['id'])
                member.append(next_member['role'])

            committee_memberships.append(member)

    return {
        'committees': committees,
        'committee_memberships': committee_memberships
    }

def fetch_committee_batch(page, updated_since):
    sleep(WAIT_TIME)

    params = BASE_PARAMS
    params['page'] = page

    if updated_since != None:
        params['updated_since'] = updated_since

    result = requests.get(url=ENDPOINTS['committees'], params=params).json()

    return result['results'], result['pagination']['max_page']

def get_committee_data(page=1, updated_since=None):
    committee_data, num_pages = fetch_committee_batch(page, updated_since)
    return process_committee_json(committee_data, updated_since), num_pages

def main():
    get_committee_data()

if __name__ == "__main__":
    main()