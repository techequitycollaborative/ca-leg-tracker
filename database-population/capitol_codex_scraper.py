import pandas as pd
import numpy as np
from typing import List, Union
import logging
logger = logging.getLogger(__name__)

SHEET_LINKS = {
    "asm": "https://docs.google.com/spreadsheets/d/1gFeGy72R_-FSFrjXbKCAAvVsvNjyV7t_TUvFoB12vys/edit?gid=1302716632#gid=1302716632",
    "sen": "https://docs.google.com/spreadsheets/d/1gFeGy72R_-FSFrjXbKCAAvVsvNjyV7t_TUvFoB12vys/edit?gid=1076436693#gid=1076436693"
}

KEYWORDS_TO_SKIP = {"VICE CHAIR", "CHAIR", "By issue area"} 

SEPARATORS = ["\+", "/", "&", ",", " and ", ";", "\\n"]

def build_sheet_url(source_url: str) -> str:
    return source_url.replace("edit?", "export?format=csv&")

def scrape_clean_sheet(chamber: str) -> pd.DataFrame:
    ### SCRAPE
    # redirect edit mode into export mode
    source = build_sheet_url(SHEET_LINKS[chamber])

    # read CSV into memory and convert to DF
    df = pd.read_csv(source)
    logger.info('Google sheet loaded as CSV and converted to DataFrame')

    ### CLEAN
    # convert district strings into integer for DB join
    if chamber == "asm":
        df["district_number"] = df["District"].str.replace("AD", "").astype(int)
    else:
        df["district_number"] = df["District"].str.replace("SD", "").astype(int)
    logger.info('Converted district identifiers to integers')
    
    # drop vacant districts
    df = df.loc[~df["Member"].str.contains("ZZ-VACANT")]
    logger.info('Removed vacant districts')

    # TODO: verify surname and district number matches OpenStates data

    # drop redundant ID columns
    df = df.drop(columns=["District", "Party", "Member"])
    logger.info('Drop redundant identification columns')
    return df

def extract_contacts(chamber: str) -> List[pd.DataFrame]:
    # Scrape sheet
    sheet_df = scrape_clean_sheet(chamber)
    results = dict()

    domain_email = f"@{chamber.lower()}.ca.gov"
    # Loop ends before the final 'district_number' column
    for issue in sheet_df.columns[:-1]:  
        logger.info(f'Processing {issue}...')

        # Skip null/empty/whitespace-only values at the start - no need to store
        valid_rows = sheet_df[issue].notna() & (sheet_df[issue].str.strip() != "")
        contacts = sheet_df.loc[valid_rows, [issue, "district_number"]].copy()
        logger.info('Filter empty rows')

        # add default staffer type
        contacts["staffer_type"] = "office"
        # update staffer type for committee specifiers
        contacts.loc[contacts[issue].str.contains("CHAIR|CMTE"), ["staffer_type"]] = "committee"
        logger.info('Extract staffer type')

        # add issue area
        contacts["issue_area"] = issue
        logger.info('Extract issue area')

        # clean and normalize names: format, specifiers, nicknames, etc.
        contacts[issue] = (
            contacts[issue]
            .str.replace(".", " ", regex=False)  # Handle internal periods "Foo.X.Bar" -> "Foo X Bar"
            .str.replace("CMTE/", "") 
            .str.replace("VICE CHAIR (-|\|) ", "", regex=True)
            .str.replace("CHAIR (-|\|) ", "", regex=True)
            .str.replace("\(\w+\) ", "", regex=True)
            .str.replace(r"\s+", " ", regex=True)  # Collapse multiple spaces
            .str.strip()
        )
        logger.info('Normalize staffer names')

        # split and explode aggregated staffers when they appear
        contacts[issue + "_values"] = contacts[issue].str.split('|'.join(SEPARATORS))
        contacts = contacts.explode(issue + "_values").drop(columns=[issue])
        logger.info('Split aggregated staffer names')

        # strip extra whitespace
        contacts[f"{issue}_values"] = contacts[f"{issue}_values"].str.strip()

        # rename column
        contacts = contacts.rename(columns={f"{issue}_values": issue})

        # Parse name into a list of parts
        split_names = contacts[issue].str.strip().str.split(" ")

        # Assign parts to columns (only first and last name, ignore middle name)
        contacts["first_name"] = split_names.str.get(0)
        contacts["last_name"] = split_names.str.get(-1)  # Always last element
        logger.info('Split staffer names into parts')

        # Generate email (first.last@chamber.ca.gov) except for skip conditions
        contacts["generated_email"] = np.where(
            contacts[issue].str.strip().isin(KEYWORDS_TO_SKIP),
            "NA",
            contacts["first_name"].str.lower() + "." + contacts["last_name"].str.lower() + domain_email
        )
        logger.info('Generate emails with NA value when skipping')

        # Rename issue column to staffer name
        contacts = contacts.rename(columns={issue: "staffer_contact"})

        # Append 
        final = contacts[["district_number", "staffer_contact", "generated_email", "issue_area", "staffer_type"]]
        results[issue] = final
        logger.info(f"{issue} extraction complete")
    return results

def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s', 
        datefmt='%m/%d/%Y %I:%M:%S %p',
        filename='codex_scraper.log', 
        level=logging.INFO
        )
    assembly = extract_contacts("sen")
    return assembly

if __name__ == "__main__":
    main()