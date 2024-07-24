# ca-leg-tracker

## `database-scripts`
Scripts in this folder are responsible for creating and dropping tables in the pSQL schema. **All DB design changes 
(ex: new fields, data type changes) need to be reflected in all `.sql` files.**

## `database-population`
Scripts in this folder handle database population from external sources (anything not created by a tool user). Database
population relies on configuration of a developer's credentials (see below). These credentials are ingested by 
`config.py` which is called by both `*_fetch.py` scripts. The fetching scripts are called by one or more `*_update.py` scripts.

Data that can't be sourced through OpenStates will be scraped by the relevant `*_scraper.py` or `*_parser.py` file. 
**The scripts in this folder are actively under construction.** Data population features are recorded in this repo's issues. 

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

[postgresql_schemas]
openstates_schema = *******
legtracker_schema = *****
frontend_user = ******
backend_user = ******
```

This will be parsed by `config.py` which enables access to the pSQL and OpenStates APIs.  