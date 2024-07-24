from config import config
import pandas as pd
import psycopg2
import openpyxl
import sys
from text_utils import transform_name

chambers = {
    "Assembly": 1,
    "Senate": 2
}

# Helper functions

def _get_link_if_exists(cell) -> str | None:
    try:
        return cell.hyperlink.target
    except AttributeError:
        return None


def extract_hyperlinks_from_xlsx(
    file_name: str, sheet_name: str, columns_to_parse: list[str], row_header: int = 1
) -> pd.DataFrame:
    df = pd.read_excel(file_name, sheet_name)
    ws = openpyxl.load_workbook(file_name)[sheet_name]
    for column in columns_to_parse:
        row_offset = row_header + 1
        column_index = list(df.columns).index(column) + 1
        df['webpage_link'] = [
            _get_link_if_exists(ws.cell(row=row_offset + i, column=column_index))
            for i in range(len(df[column]))
        ]
    return df


def get_raw_data(filename, chamber_name):
    return extract_hyperlinks_from_xlsx(
        filename,
        chamber_name,
        ['Committee']
    )


def excel_to_committee_table(df, c):
    result = df.loc[:, ["Committee", "webpage_link"]]
    result.rename(columns={"Committee": "name"}, inplace=True)
    result["chamber_id"] = chambers[c]
    result = result[["chamber_id", "name", "webpage_link"]]
    output_name = f"{c.lower()}_committee_rows.csv"
    result.to_csv(output_name, index=False)
    return
        

def excel_to_committee_assignment(df, c, legislator_db, committee_db):
    assignments = list()
    chamber_id = chambers[c]
    for i in range(len(df)):
        committee_name = df.iloc[i, 0]
        for j in range(1, len(df.columns)):
            cell = df.iloc[i, j]
            if j == 1:
                curr = [committee_name, chamber_id, cell, "Chair"]
            elif j == 2:
                curr = [committee_name, chamber_id, cell, "Vice Chair"]
            elif type(cell) == str and "Member" in df.columns[j]:
                curr = [committee_name, chamber_id, cell, "Member"]
            else:
                continue
            assignments.append(curr)
    result = pd.DataFrame(assignments, columns=['name', 'chamber_id', 'legislator_name', 'assignment_type'])
    result['legislator_name'] = result['legislator_name'].map(lambda n: transform_name(n))
    result_committee_id = (result.merge(committee_db, on='name').reindex(
        columns=['committee_id', 'name', 'chamber_id', 'legislator_name', 'assignment_type']
    ))

    if len(result) != len(result_committee_id):
        print("PLACEHOLDER ERROR MESSAGE")
        sys.exit(1)
    
    result_both_ids = (result_committee_id.merge(legislator_db, on='legislator_name').reindex(
        columns=['committee_id', 'name', 'chamber_id', 'legislator_name', 'assignment_type', 'legislator_id']
    ))

    if len(result_committee_id) != len(result_both_ids):
        print("PLACEHOLDER ERROR MESSAGE")
        sys.exit(1)

# Database connection functions

def legtracker_upsert_committee(cur, committees):
    # TODO: upsert results of excel_to_committee_table into DB
    # IFF this executes successfully, we can parse committee assignments (We need the serial pkey)
    return


def legtracker_insert_committee_assignment(cur, assignments):
    # TODO: insert results of excel_to_committee_assignment into DB
    return
    

def main(filename):
    conn = None
    try:
        # read connection parameters
        params = config("postgresql")

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        for c in chambers.keys():
            df = extract_hyperlinks_from_xlsx(filename, c)
            excel_to_committee_table(df, c)
            legtracker_upsert_committee()

        # fetch legislators and committees from legtracker
        print('Fetching legislators and committees')
        # TODO: fetch legislator table from DB and convert to DF
        legislator_db = pd.read_csv("PLACEHOLDER.CSV")
        legislator_db.rename(columns={"name": "legislator_name"}, inplace=True)
        legislator_db = legislator_db.loc[:, ['legislator_id', 'legislator_name']]
        # TODO: fetch fresh committee table from DB (with serial pkey) and convert to DF
        committee_db = pd.read_csv("PLACEHOLDER.csv")
        committee_db = committee_db.loc[:, ['committee_id', 'name']]

        # parse excel content

        for c in chambers.keys():
            excel_to_committee_assignment(df, c, legislator_db, committee_db)
            legtracker_insert_committee_assignment()

        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Failed to update records", error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed')

    print('Update finished')


if __name__ == "__main__":
    main(sys.argv[1])
