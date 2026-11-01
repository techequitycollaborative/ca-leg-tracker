import psycopg2
import pytest

import db
from tests.conftest import SNAPSHOT_SCHEMA, APP_SCHEMA


def test_commit_is_blocked(rollback_cur):
    conn = rollback_cur.connection
    with pytest.raises(RuntimeError):
        conn.commit()
    with pytest.raises(RuntimeError):
        conn.autocommit = True
    with pytest.raises(RuntimeError):
        conn.set_session(autocommit=True)
    # db.get_cursor() commits, so it must not be able to connect
    with pytest.raises(RuntimeError):
        with db.get_cursor():
            pass
    with pytest.raises(RuntimeError):
        psycopg2.connect(dbname="unused")


def test_connection_and_version(rollback_cur):
    rollback_cur.execute("SELECT version(), current_setting('server_version_num')::int")
    version, version_num = rollback_cur.fetchone()
    print(f"\nServer: {version} (server_version_num={version_num})")
    assert version_num >= 140000
    assert rollback_cur.connection.autocommit is False


def test_list_deferred_constraints(rollback_cur):
    # Deferred checks normally fire at COMMIT; conftest forces them IMMEDIATE.
    # Informational: list what that applies to
    rollback_cur.execute(
        """
        SELECT conrelid::regclass::text, conname
        FROM pg_constraint
        WHERE condeferrable AND connamespace::regnamespace::text = ANY(%s)
        UNION ALL
        SELECT tgrelid::regclass::text, tgname
        FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
        WHERE t.tgdeferrable AND NOT t.tgisinternal
            AND c.relnamespace::regnamespace::text = ANY(%s)
        """,
        ([SNAPSHOT_SCHEMA, APP_SCHEMA], [SNAPSHOT_SCHEMA, APP_SCHEMA]),
    )
    rows = rollback_cur.fetchall()
    print(f"\nDeferrable constraints/triggers: {rows or 'none'}")
