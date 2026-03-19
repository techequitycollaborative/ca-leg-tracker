from config import config
from io import StringIO
import csv
import session.sources.capitol_codex_scraper as codex

# Index into credentials.ini for DB schema names
SNAPSHOT_SCHEMA = config("postgresql_schemas")["snapshot_schema"]
# TODO: convert to YAML
CONTACTS_COLUMNS = ["openstates_people_id", "staffer_contact", "generated_email", "issue_area", "staffer_type"]

LAST_UPDATED_DEFAULT = "2000-01-01T00:00:00"

def fetch_codex_updates():
    print("Extracting current Capitol Codex data...")
    assembly_update = codex.extract_contacts("asm")
    senate_update = codex.extract_contacts("sen")

    return {
        "lower": assembly_update,
        "upper": senate_update
    }


def codex_upsert_contacts(
        cur,
        contact_data,
        chamber
):
    temp_table_name = "contacts_temp"
    temp_table_query = """
        DROP TABLE IF EXISTS {0};
        CREATE TEMPORARY TABLE {0} (
        district_number INT,
        staffer_contact TEXT,
        generated_email TEXT,
        issue_area TEXT,
        staffer_type TEXT
        )
    """  # specifying columns because they differ from the final people_contacts table
    
    cur.execute(temp_table_query.format(temp_table_name, SNAPSHOT_SCHEMA))

    # Insert contacts collected for each issue to the staging table
    for issue, df in contact_data.items():
        print(f"Processing {issue}...")
        if df.empty:
            print(f"Skipping {issue} (empty DataFrame)")
            continue

        try:
            buffer = StringIO()

            df.to_csv(buffer, index=False, header=False, sep="\t", quoting=csv.QUOTE_NONE, escapechar='\\')
            buffer.seek(0)

            cur.copy_expert(
                sql="COPY {0} FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t')".format(temp_table_name),
                file=buffer
                )
            print(f"Staged {len(df)} rows for {issue}")
        except Exception as e:
                print(f"[CONTACTS] ERROR processing {issue}: {str(e)}")                
        finally:
            buffer.close()
    
    # Final bulk insert
    print("Inserting from temp to final table...")

    insert_query = """
        INSERT INTO {0}.people_contacts (openstates_people_id, staffer_contact, generated_email, issue_area, staffer_type)
        SELECT 
            pr.openstates_people_id,
            t.staffer_contact,
            t.generated_email,
            t.issue_area,
            t.staffer_type
        FROM {1} t
        JOIN {0}.people_roles pr ON t.district_number = pr.district AND pr.org_classification='{2}'
    """
    cur.execute(insert_query.format(SNAPSHOT_SCHEMA, temp_table_name, chamber))
    print(cur.statusmessage)
    return