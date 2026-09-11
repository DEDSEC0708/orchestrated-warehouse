"""The pipeline must refuse to start against an un-initialised warehouse.

The container bootstrap creates roles, databases and extensions; the warehouse
schema is a separate, deliberate step (``make db-init``, bundled into
``make run-clean``). Before this check existed, skipping it produced

    psycopg.errors.UndefinedTable:
    relation "audit.pipeline_run" does not exist

fifteen frames into the first INSERT - a true statement that says nothing about
the missing prerequisite. These tests pin the diagnosis, not just the failure.

The ``cms`` database stands in for "a database the bootstrap created but the
DDL never touched": it genuinely has none of the seven warehouse schemas, and
the test roles are allowed to reach it. Creating a throwaway database instead
would need CREATEDB, which every role in this project is deliberately denied.
"""

from __future__ import annotations

import psycopg
import pytest
from tests.conftest import requires_database

from volthive.db.preflight import (
    REQUIRED_SCHEMAS,
    SENTINEL_TABLE,
    require_initialised_warehouse,
)
from volthive.exceptions import ConfigurationError, ContractError

pytestmark = [pytest.mark.integration, requires_database]


def _dsn(env: dict[str, str], user_key: str, password_key: str, database: str) -> str:
    return (
        f"postgresql://{env[user_key]}:{env[password_key]}"
        f"@{env['POSTGRES_HOST']}:{env['POSTGRES_PORT']}/{database}"
    )


@pytest.fixture
def uninitialised_db(integration_env: dict[str, str]):
    """A real database with no warehouse schema in it."""
    dsn = _dsn(integration_env, "CMS_READER_USER", "CMS_READER_PASSWORD", integration_env["CMS_DB"])
    with psycopg.connect(dsn) as conn:
        yield conn


@pytest.fixture
def half_applied_db(integration_env: dict[str, str]):
    """Schemas created, tables not - the state a partial DDL run leaves."""
    dsn = _dsn(integration_env, "CMS_OWNER_USER", "CMS_OWNER_PASSWORD", integration_env["CMS_DB"])
    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            for schema in REQUIRED_SCHEMAS:
                conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            yield conn
        finally:
            for schema in REQUIRED_SCHEMAS:
                conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_an_empty_database_is_rejected_with_the_remedy(uninitialised_db) -> None:
    """The error must name what is missing AND the command that fixes it."""
    with pytest.raises(ConfigurationError) as caught:
        require_initialised_warehouse(uninitialised_db)

    message = str(caught.value)
    for schema in REQUIRED_SCHEMAS:
        assert schema in message, f"the error does not mention the missing {schema!r} schema"
    assert "make db-init" in message, "the error does not name the command that fixes it"
    assert "make run-clean" in message

    # A configuration error must never be retried into working: the next
    # attempt fails identically, and a retrying task hides the cause.
    assert caught.value.is_retryable is False
    assert isinstance(caught.value, ContractError)


def test_a_half_applied_schema_is_rejected(half_applied_db) -> None:
    """`CREATE SCHEMA` succeeding says nothing about the DDL that fills it.

    A schema-only check would pass here and then fail on whichever table
    happened to be missing, which is the original problem again.
    """
    with pytest.raises(ConfigurationError) as caught:
        require_initialised_warehouse(half_applied_db)
    assert SENTINEL_TABLE in str(caught.value)


def test_an_initialised_warehouse_passes(warehouse_conn) -> None:
    """The built warehouse must satisfy its own precondition."""
    require_initialised_warehouse(warehouse_conn)


def test_the_check_works_whatever_row_factory_the_caller_used(
    uninitialised_db, warehouse_conn
) -> None:
    """It must not inherit the caller's row factory.

    ``warehouse_connection()`` yields ``dict_row``; a plain ``psycopg.connect()``
    yields tuples. A check that reads its own result differently depending on
    how the caller was constructed is a check you cannot trust - and reading a
    dict_row positionally yields the COLUMN NAMES, producing a confidently
    wrong error naming schemas 'm', 'i', 's'.
    """
    # tuple_row connection (the fixture uses psycopg.connect directly)
    with pytest.raises(ConfigurationError) as caught:
        require_initialised_warehouse(uninitialised_db)
    assert "audit" in str(caught.value) and "missing schemas" in str(caught.value)

    # dict_row connection - must simply pass, not misread its own row
    require_initialised_warehouse(warehouse_conn)


def test_every_required_schema_is_actually_created_by_the_ddl(warehouse_conn) -> None:
    """REQUIRED_SCHEMAS must match what sql/ddl/00_schemas.sql produces.

    If the DDL gains a schema and this tuple does not, the check stops covering it.
    """
    with warehouse_conn.cursor() as cur:
        cur.execute(
            "SELECT nspname FROM pg_namespace WHERE nspname = ANY(%(names)s)",
            {"names": list(REQUIRED_SCHEMAS)},
        )
        present = {row["nspname"] for row in cur.fetchall()}
    assert present == set(REQUIRED_SCHEMAS)
