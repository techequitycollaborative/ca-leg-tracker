"""
"""
from config import config

# Index into credentials.ini for globals
APP_SCHEMA = config("postgresql_schemas")["app_schema"]

def refresh(cur):
    bills_query = """
        REFRESH MATERIALIZED VIEW CONCURRENTLY {0}.bills_mv
    """
    bill_history_query = """
        REFRESH MATERIALIZED VIEW CONCURRENTLY {0}.bill_history_mv
    """
    # TODO: include legislators, which should also be a materialized view

    print("Refreshing materialized view - bills")
    cur.execute(bills_query.format(APP_SCHEMA))
    print(cur.statusmessage)

    print("Refreshing materialized view - bill history")
    cur.execute(bill_history_query.format(APP_SCHEMA))
    print(cur.statusmessage)
    return