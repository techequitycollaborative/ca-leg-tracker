"""
Shared fixtures for DB-write regression tests (PG14 -> PG17 upgrade).

Every test runs against the real cluster in credentials.ini inside a single
transaction that is ALWAYS rolled back. Commits are made impossible:
  1. NoCommitConnection.commit() raises, and autocommit cannot be enabled
  2. psycopg2.connect is patched to refuse any other connection type, so
     db.get_cursor() (which commits) can never connect during tests
  3. Teardown always rolls back and closes the connection
"""

import os
import sys
import uuid
import datetime as dt
from contextlib import contextmanager
from pathlib import Path

# Snapshot modules read credentials.ini / request_config.yaml relative to the
# working directory at import time, so pin it before importing them
ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

import pandas as pd
import psycopg2
import psycopg2.extensions
import pytest

import db
from config import config
from snapshots import bills, hearings

SNAPSHOT_SCHEMA = config("postgresql_schemas")["snapshot_schema"]
APP_SCHEMA = config("postgresql_schemas")["app_schema"]
TEST_PREFIX = "pgupgrade-test-"

_real_connect = psycopg2.connect


class NoCommitConnection(psycopg2.extensions.connection):
    def commit(self):
        raise RuntimeError("commit() is disabled in DB-write tests")

    def set_session(self, *args, **kwargs):
        if kwargs.get("autocommit"):
            raise RuntimeError("autocommit is disabled in DB-write tests")
        return super().set_session(*args, **kwargs)

    @property
    def autocommit(self):
        return psycopg2.extensions.connection.autocommit.__get__(self)

    @autocommit.setter
    def autocommit(self, value):
        if value:
            raise RuntimeError("autocommit is disabled in DB-write tests")
        psycopg2.extensions.connection.autocommit.__set__(self, value)


def _guarded_connect(*args, **kwargs):
    if kwargs.get("connection_factory") is not NoCommitConnection:
        raise RuntimeError(
            "Only NoCommitConnection may connect during DB-write tests"
        )
    return _real_connect(*args, **kwargs)


@pytest.fixture(autouse=True)
def block_committing_connections(monkeypatch):
    monkeypatch.setattr(psycopg2, "connect", _guarded_connect)


@pytest.fixture
def rollback_cur(block_committing_connections):
    conn = psycopg2.connect(
        **config("postgres"), connection_factory=NoCommitConnection
    )
    try:
        cur = conn.cursor()
        cur.execute("SET LOCAL statement_timeout = '60s'")
        cur.execute("SET LOCAL lock_timeout = '10s'")
        # Check deferred constraints per statement, since COMMIT never runs
        cur.execute("SET CONSTRAINTS ALL IMMEDIATE")
        yield cur
    finally:
        conn.rollback()
        conn.close()


@pytest.fixture
def patched_db(monkeypatch, rollback_cur):
    @contextmanager
    def get_cursor():
        yield rollback_cur

    monkeypatch.setattr(db, "get_cursor", get_cursor)
    return rollback_cur


def drop_temp_tables(cur, *tables):
    for table in tables:
        cur.execute(f"DROP TABLE IF EXISTS pg_temp.{table}")


@pytest.fixture
def marker():
    return TEST_PREFIX + uuid.uuid4().hex[:8]


@pytest.fixture
def bill_updates(marker):
    bill_id = marker
    bill_num = f"ZZ {marker}"
    return {
        "bills": pd.DataFrame(
            [
                [
                    bill_id,
                    hearings.CURRENT_SESSION,
                    "Assembly",
                    bill_num,
                    f"{marker} title",
                    "2025-01-01T00:00:00+00:00",
                    "2025-01-02T00:00:00+00:00",
                    "2025-01-01",
                    "2025-01-02",
                    f"{marker} abstract",
                ]
            ],
            columns=bills.BILL_COLUMNS,
        ),
        "bill_actions": pd.DataFrame(
            [[bill_id, "Assembly", f"{marker} introduced", "2025-01-01", "1"]],
            columns=bills.BILL_ACTION_COLUMNS,
        ),
        "bill_sponsors": pd.DataFrame(
            [[bill_id, "Tester", "Test Tester", "Assemblymember", "99", "True", "primary"]],
            columns=bills.BILL_SPONSOR_COLUMNS,
        ),
        "bill_votes": pd.DataFrame(
            [[bill_id, f"{marker} motion", "2025-01-02", "Assembly", "pass", "majority", "41", "0", "0"]],
            columns=bills.BILL_VOTE_COLUMNS,
        ),
    }


@pytest.fixture
def hearing_date():
    return dt.date.today() + dt.timedelta(days=30)


@pytest.fixture
def hearing_schedule(marker, hearing_date):
    # Tuple order matches utils.scraping.normalize_scraper_results
    return {
        (
            1,
            f"{marker} Committee",
            hearing_date,
            "9 a.m.",
            "09:00",
            False,
            "1021 O Street",
            "Room 1100",
            None,
        )
    }


@pytest.fixture
def bill_schedule(marker, hearing_date):
    # Tuple order matches utils.scraping.normalize_scraper_results
    return {
        (
            1,
            hearing_date,
            f"{marker} Committee",
            "9 a.m.",
            "1021 O Street",
            "Room 1100",
            f"ZZ {marker}",
            1,
            None,
            None,
        )
    }


@pytest.fixture
def lower_district(rollback_cur):
    rollback_cur.execute(
        f"""
        SELECT openstates_people_id, district
        FROM {SNAPSHOT_SCHEMA}.people_roles
        WHERE org_classification = 'lower' AND district IS NOT NULL
        ORDER BY district
        LIMIT 1
        """
    )
    row = rollback_cur.fetchone()
    if row is None:
        pytest.skip("No lower-chamber people_roles row to join contacts against")
    return row


@pytest.fixture
def contact_updates(marker, lower_district):
    # Column order matches capitol_codex_scraper.extract_contacts
    _, district = lower_district
    df = pd.DataFrame(
        [[district, f"{marker} Staffer", f"{marker}@asm.ca.gov", "Testing", "staff"]],
        columns=[
            "district_number",
            "staffer_contact",
            "generated_email",
            "issue_area",
            "staffer_type",
        ],
    )
    return {"lower": {"Testing": df}}
