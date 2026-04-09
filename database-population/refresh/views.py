""" """

from config import config
import time
import logging

logger = logging.getLogger(__name__)
# Index into credentials.ini for globals
APP_SCHEMA = config("postgresql_schemas")["app_schema"]

# Refresh order matters — dependencies first
MATERIALIZED_VIEWS = [
    "bills_mv",
    "bill_history_mv",
    "committees_mv",
    "calendar_mv",
]


def refresh(cur):
    for view in MATERIALIZED_VIEWS:
        try:
            logger.info(f"Refreshing materialized view - {view}")
            start = time.time()
            cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {APP_SCHEMA}.{view}")
            elapsed = time.time() - start
            logger.info(f"{cur.statusmessage} ({elapsed:.2f}s)")
        except Exception as e:
            logger.error(f"ERROR refreshing {view}: {e}")
