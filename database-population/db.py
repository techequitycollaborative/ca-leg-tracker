"""
Database connection management for the data pipeline.
Provides a context manager for psycopg2 cursor lifecycle and
a retry wrapper for unreliable external API calls.

NOTE: connection logic is duplicated in session/session_update.py
Consolidate into common DB module references when integrating full stack
services.
"""

from contextlib import contextmanager
import psycopg2
from config import config

# import time
# import logging

# logger = logging.getLogger(__name__)


@contextmanager
def get_cursor():
    conn = None
    try:
        params = config("postgres")
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        yield cur
        conn.commit()
        # logger.info("Transaction committed")
        print("Transaction committed")
    except psycopg2.DatabaseError as e:
        if conn:
            conn.rollback()
            # logger.error(f"Transaction rolled back: {e.pgerror}")
            print(f"Transaction rolled back: {e.pgerror}")
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            # logger.error(f"Transaction rolled back: {str(e)}")
            print(f"Transaction rolled back: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()
            # logger.info("Database connection closed")
            print("Database connection closed")
