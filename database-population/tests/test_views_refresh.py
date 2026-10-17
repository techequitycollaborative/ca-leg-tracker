import warnings

import pytest

from refresh import views
from tests.conftest import APP_SCHEMA

VIEWS = list(views.MATERIALIZED_VIEWS.values())

# PG17+ runs REFRESH MATERIALIZED VIEW (and VACUUM/ANALYZE/etc.) with this
# search_path, which breaks functions relying on unqualified names
PG17_MAINTENANCE_SEARCH_PATH = "pg_catalog, pg_temp"


@pytest.mark.parametrize("view", VIEWS)
def test_refresh_view(rollback_cur, view):
    # Run directly: views.refresh() swallows errors
    rollback_cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {APP_SCHEMA}.{view}")


@pytest.mark.parametrize("view", VIEWS)
def test_refresh_view_restricted_search_path(rollback_cur, view):
    # Emulates PG17 refresh behavior on PG14 so breakage shows before upgrading
    rollback_cur.execute(f"SET LOCAL search_path = {PG17_MAINTENANCE_SEARCH_PATH}")
    rollback_cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {APP_SCHEMA}.{view}")


def test_matview_function_dependencies(rollback_cur):
    # Informational: user-defined functions the views call that don't pin
    # their own search_path (candidates for PG17 breakage)
    rollback_cur.execute(
        """
        SELECT DISTINCT mv.relname, p.oid::regprocedure::text
        FROM pg_class mv
        JOIN pg_rewrite r ON r.ev_class = mv.oid
        JOIN pg_depend d
            ON d.classid = 'pg_rewrite'::regclass
            AND d.objid = r.oid
            AND d.refclassid = 'pg_proc'::regclass
        JOIN pg_proc p ON p.oid = d.refobjid
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE mv.relkind = 'm'
            AND mv.relnamespace::regnamespace::text = %s
            AND mv.relname = ANY(%s)
            AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND NOT EXISTS (
                SELECT 1 FROM unnest(p.proconfig) c WHERE c LIKE 'search_path=%%'
            )
        ORDER BY 1, 2
        """,
        (APP_SCHEMA, VIEWS),
    )
    rows = rollback_cur.fetchall()
    print(f"\nMatview functions without pinned search_path: {rows or 'none'}")
    if rows:
        warnings.warn(f"Matview functions without pinned search_path: {rows}")
