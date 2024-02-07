# ca-leg-tracker

## `database-scripts`
Scripts in this folder are responsible for creating and dropping tables in the pSQL schema. **All design changes 
(ex: new fields, data type changes) need to be reflected in `create-tables.sql` and `drop-tables.sql`.**

## `database-population`
Scripts in this folder handle database population from external sources (anything not created by a tool user). Database
population relies on configuration of a developer's credentials (see below). These credentials are ingested by 
`config.py` which is called by `api_requests.py`. As the name suggests, `api_requests.py` defines all unique requests 
that can be made to the OpenStates API. 

Data that can't be sourced through OpenStates will be scraped by the relevant `*_scraper.py` file. The 
data requests that are defined through `api_requests.py` and all `*_scraper.py` files are called by at least one of the 
two main population scripts: `daily.py` and `legislative_session.py`. These are named for the frequency by which the 
data needs to be refreshed with. **The scripts in this folder are actively under construction.** Data population
features are recorded in this repo's issues. 

## Credentials
Locally configure a `credentials.ini` file. For example:
```
[openstates]
api_key = **********

[postgresql]
user = *********
password = *********
host = *********.db.ondigitalocean.com
port = *********
dbname = *********
sslmode = require
```

This will be parsed by `config.py` which enables access to the pSQL and OpenStates APIs.  

## Manual Database Population (INTERIM)
*NOTE: Internal testing has revealed the OpenStates daily request limit is actually ~250.*

### Populate Today's Bill Data
Adding a `--today` flag filters the request to OpenStates API for bills created on the current date.
```commandline
cd database-population
python manual_daily.py --today
```
### Populate Bill Data by Name
Adding a `--named` flag filters the request to OpenStates API for the bill(s) from the current session whose "AB X" or 
"SB Y"-format names have been added to a helper file, `bill_request_list.txt`, which should be tracked locally. 
```commandline
cd database-population
python manual_daily.py --named
```
