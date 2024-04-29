# ca-leg-tracker
The scripts in this repository can be executed to initialize and maintain a postgreSQL database that populates and 
transforms data from various government websites so that they can be managed in a streamlined web tool.

## Using Issues
Progress and assignment of data population/transformation features and bugs are managed in the issues of this 
repository. The corresponding progress for front-end issues will be similarly managed in the `ca-leg-tracker-frontend`
repository's issues.

## `database-scripts`
Scripts in this folder are responsible for creating and dropping tables in the pSQL schema. **All design changes 
(ex: new fields, data type changes) need to be reflected in these `*.sql` files.**

## `database-population`
Scripts in this folder handle database population from external sources. Database population relies on configuration of 
a developer's credentials (see below). 

Data that can't be sourced through OpenStates will be scraped by the relevant `*_scraper.py` file. The 
data requests that are defined through `bill_openstates_fetch.py` and all `*_scraper.py` files are called by at least 
one of the two main population scripts: `bill_daily_update.py` and `legislative_session.py`. These are named for the 
frequency by which the data needs to be refreshed with. 


### Credentials
In the `database-population` folder, configure a `credentials.ini` file. Use the `credentials.ini.example` file as a 
template.

This will be parsed by `config.py` which enables access to the pSQL and OpenStates APIs.  

### `bill_daily_update.py`
This script is run on a `cron` job to update the database with new bills and associated information. 

### `bill_openstates_fetch.py`
This script directly parses the JSON output of the OpenStates API for the `snapshot` schema. 

### `committee_scraper,py`
This script scrapes the names of all Assembly committees and their members. NOTE: Senate committee membership can only 
be manually collected.

### `legislative_session.py`
Once after the legislative session begins, this script should be run on a `cron` job to update the database's legislator
and committee-related information.  

### `scraper_utils.py` (WIP)
This file defines miscellaneous methods needed for the custom scrapers. 

## `db` (WIP)
Scripts in this folder define objects to be accessed by the front-end repository, `ca-leg-tracker-frontend`.


