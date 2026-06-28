""" """

from config import config
import time
import logging
import db

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Index into credentials.ini for globals
APP_SCHEMA = config("postgresql_schemas")["app_schema"]

# Refresh order matters — dependencies first
MATERIALIZED_VIEWS = {
    "bills": "bills_mv",
    "actions": "bill_history_mv",
    "committees": "committees_mv",
    "hearings": "hearings_mv",
    "scheduled bills": "hearing_bills_mv",
    "letter deadlines": "hearing_deadlines_mv"
}

def refresh(cur):
    for atom, view in MATERIALIZED_VIEWS.items():
        try:
            logger.info(f"Refreshing materialized view - {view}")
            start = time.time()
            cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {APP_SCHEMA}.{view}")
            elapsed = time.time() - start
            logger.info(f"{cur.statusmessage} ({elapsed:.2f}s)")
            cur.execute(f"SELECT COUNT(*) FROM {APP_SCHEMA}.{view}")
            atomic_count = cur.fetchone()[0]
            logger.info(f"{atom.title()} visible in mat view: {atomic_count}")
        except Exception as e:
            logger.error(f"ERROR refreshing {view}: {e}")

if __name__ == "__main__":
    with db.get_cursor() as cur:
        refresh(cur)
