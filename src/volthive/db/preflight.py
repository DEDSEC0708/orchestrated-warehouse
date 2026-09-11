"""Assert that the warehouse has been initialised before using it.

WHY THIS EXISTS

    The PostgreSQL container bootstrap creates roles, databases and extensions
    (``docker/postgres/init/*.sh``). It deliberately does NOT create the
    warehouse schema - that is ``sql/ddl/**``, applied by
    ``scripts/apply_schema.py`` via ``make db-init``. Infrastructure bootstrap
    and schema creation are separate concerns with separate failure modes, and
    ``docker/airflow/init.sh`` says so explicitly.

    The consequence is that a freshly started stack has an empty ``warehouse``
    database, and the first thing the pipeline does is insert into
    ``audit.pipeline_run``. Without this check that surfaces as:

        psycopg.errors.UndefinedTable:
        relation "audit.pipeline_run" does not exist

    fifteen frames deep, naming a table the reader has never heard of and
    saying nothing about what to do. The prerequisite is real and documented;
    what was missing is any check that it had been met.

    So this is not a workaround for the separation - it enforces it. The
    initialisation step stays exactly where it was; it just stops being
    possible to skip it silently.

WHY NOT APPLY THE SCHEMA AUTOMATICALLY

    Because a data-loading command that quietly migrates the database it is
    about to write to is how a schema change reaches production without anyone
    deciding to make it. ``make db-init`` is one command, it is idempotent, and
    ``make run-clean`` already bundles it. Making the failure legible is the
    fix; making it invisible is not.
"""

from __future__ import annotations

import psycopg
from psycopg.rows import dict_row

from volthive.exceptions import ConfigurationError

#: The seven schemas sql/ddl/00_schemas.sql creates. Named here so a partially
#: applied schema is caught as precisely as a completely absent one.
REQUIRED_SCHEMAS: tuple[str, ...] = ("raw", "stg", "core", "mart", "dq", "audit", "ctl")

#: The first table the pipeline touches. Checked in addition to the schemas
#: because `CREATE SCHEMA` succeeding says nothing about whether the DDL that
#: fills it did - and a half-applied warehouse should fail here, with an
#: explanation, rather than at whichever table happens to be missing.
SENTINEL_TABLE = "audit.pipeline_run"


def require_initialised_warehouse(conn: psycopg.Connection) -> None:
    """Raise :class:`ConfigurationError` if the warehouse DDL has not been applied.

    One query, run once per pipeline run rather than per task, so the cost is
    a rounding error against the work that follows.
    """
    # Ask for dict_row explicitly rather than inheriting the caller's factory.
    # warehouse_connection() uses dict_row, but a plain psycopg.connect() gives
    # tuples, and a check that behaves differently depending on how its caller
    # was constructed is a check you cannot trust. Same pattern as
    # volthive.db.connection.fetch_all.
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                coalesce(
                    array_agg(missing.name ORDER BY missing.name)
                        FILTER (WHERE NOT missing.present),
                    ARRAY[]::TEXT[]
                ) AS missing_schemas,
                to_regclass(%(sentinel)s) IS NOT NULL AS sentinel_present,
                current_database() AS database
            FROM (
                SELECT
                    s.name,
                    EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspname = s.name) AS present
                FROM unnest(%(schemas)s::TEXT[]) AS s(name)
            ) AS missing
            """,
            {"schemas": list(REQUIRED_SCHEMAS), "sentinel": SENTINEL_TABLE},
        )
        row = cur.fetchone()

    if row is None:  # pragma: no cover - a single-row aggregate always returns
        return

    # Read by key. Unpacking a dict_row positionally silently yields the
    # COLUMN NAMES instead of the values, which produces a confident and
    # entirely wrong error naming schemas 'm', 'i', 's', 's', 'i', 'n', 'g'.
    missing_schemas = row["missing_schemas"]
    sentinel_present = row["sentinel_present"]
    database = row["database"]

    if not missing_schemas and sentinel_present:
        return

    if missing_schemas:
        detail = f"missing schemas: {', '.join(missing_schemas)}"
    else:
        detail = f"{SENTINEL_TABLE} does not exist"

    raise ConfigurationError(
        f"The warehouse schema has not been applied to database "
        f"{database!r} ({detail}).\n"
        f"\n"
        f"The database, roles and extensions are created when the stack "
        f"starts; the schema is a separate, deliberate step.\n"
        f"\n"
        f"  make db-init     apply the schema, seeds and data-quality rules\n"
        f"  make run-clean   db-init, generate and run, from an empty database\n"
        f"\n"
        f"`make run` on its own assumes both have already happened.",
        entity=database,
        expected="warehouse schema applied",
        actual=detail,
    )
