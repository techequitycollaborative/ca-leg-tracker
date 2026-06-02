from config import config
from io import StringIO
import csv
import sources.capitol_codex_scraper as codex
from yaml import safe_load
import logging

logger = logging.getLogger(__name__)

# Index into credentials.ini for DB schema names
SNAPSHOT_SCHEMA = config("postgresql_schemas")["snapshot_schema"]
REQUEST_CONFIG = safe_load(open(config("resources")["request_config"]))
CONTACTS_COLUMNS = REQUEST_CONFIG["CONTACTS_COLUMNS"]


def fetch_updates():
    logger.info("Fetching Capitol Codex...")
    assembly_update = codex.extract_contacts("asm")
    senate_update = codex.extract_contacts("sen")

    return {"lower": assembly_update, "upper": senate_update}


def update(cur, contact_data, chamber):
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
        logger.debug(f"Issue: {issue}")
        if df.empty:
            logger.debug(f"Skipping {issue} (empty DataFrame)")
            continue

        try:
            buffer = StringIO()

            df.to_csv(
                buffer,
                index=False,
                header=False,
                sep="\t",
                quoting=csv.QUOTE_NONE,
                escapechar="\\",
            )
            buffer.seek(0)

            cur.copy_expert(
                sql="COPY {0} FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t')".format(
                    temp_table_name
                ),
                file=buffer,
            )
            logger.debug(f"Staged {len(df)} rows for {issue}")
        except Exception as e:
            logger.error(f"[CONTACTS] ERROR processing {issue}: {str(e)}")
        finally:
            buffer.close()

    logger.info(f"[{chamber}] Staged snapshot")

    flush_query = """
        DELETE FROM {0}.people_contacts pc
        USING {0}.people_roles pr, {1} t
        WHERE pc.openstates_people_id = pr.openstates_people_id
            AND t.district_number = pr.district
            AND pr.org_classification='{2}'
    """
    cur.execute(flush_query.format(SNAPSHOT_SCHEMA, temp_table_name, chamber))
    # Flush snapshot
    logger.info(f"[{chamber}] Flushing snapshot: {cur.rowcount} rows affected")

    # Final bulk insert
    logger.info("Inserting from temp to final table...")

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
    logger.info(f"Updated people_contacts snapshot: {cur.rowcount} rows affected")
    return
