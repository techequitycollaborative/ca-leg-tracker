# ca-leg-tracker

:warning: _NOTE: This repository is no longer under active development. The content represents v1 efforts to develop a legislation tracker by TechEquity's Civic Tech volunteer team._ :warning:

## `database-scripts`
Scripts in this folder generate the postgreSQL schemas and tables as the back-end of a legislation tracker. 

**Reminder: make sure that all DB design changes (ex: new fields, data type changes) are reflected in the .SQL files**

## `database-population`
Scripts in this folder handle database population from external sources (anything not created by a tool user). Database
population relies on configuration of a developer's credentials (see below). These credentials are ingested by 
`config.py` which is called by both `*_fetch.py` scripts. The results of the fetching scripts are used by one or more `*_update.py` scripts.

Data that can't be sourced through OpenStates will be scraped by the relevant `*_scraper.py` or `*_parser.py` file. 

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
