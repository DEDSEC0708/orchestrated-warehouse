"""Database connections, transactions and session configuration.

Three rules, each of which exists because of a specific failure mode:

**One connection per TASK, not per row and not per module.** A module-level
connection object leaks across Airflow task boundaries: under LocalExecutor the
scheduler forks, and a connection inherited by a child process is a connection
two processes now think they own. Every entry point here is a context manager
that opens and closes within one unit of work.

**Explicit transaction boundaries.** ``autocommit`` is left on at the
connection level and transactions are opened deliberately with
:func:`transaction`. The commit point is then visible in the code rather than
implied by whichever statement happened to run last - which matters enormously
here, because "the watermark advances in the same transaction as the data" is a
correctness claim that has to be readable to be trusted.

**A statement timeout on every session.** A runaway query holding locks
indefinitely is one of the few ways this pipeline could take the warehouse down
rather than merely fail. The timeout turns that into a failed task.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

from volthive.config.settings import get_settings
from volthive.exceptions import TransientError
from volthive.logging_setup import get_logger

__all__ = [
    "connect",
    "warehouse_connection",
    "cms_connection",
    "transaction",
    "fetch_all",
    "fetch_one",
    "fetch_value",
    "DEFAULT_STATEMENT_TIMEOUT_MS",
]

log = get_logger(__name__)

#: Thirty minutes. Long enough for the heaviest fact load over a wide
#: restatement window on a laptop, short enough that a genuinely stuck
#: statement fails inside one Airflow task timeout rather than outliving it.
DEFAULT_STATEMENT_TIMEOUT_MS = 30 * 60 * 1000

#: Errors that mean "the world was briefly unavailable" rather than "your SQL
#: is wrong". Classified as transient so the caller retries instead of
#: quarantining, per the taxonomy in volthive.exceptions.
_TRANSIENT_PG_ERRORS = (
    psycopg.OperationalError,
    psycopg.errors.AdminShutdown,
    psycopg.errors.CannotConnectNow,
    psycopg.errors.ConnectionException,
    psycopg.errors.TooManyConnections,
)


@contextmanager
def connect(
    dsn: str,
    *,
    application_name: str = "volthive",
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS,
    autocommit: bool = True,
    row_factory: Any = dict_row,
) -> Iterator[psycopg.Connection]:
    """Open a configured connection and guarantee it is closed.

    Args:
        dsn: PostgreSQL connection URI.
        application_name: Shows up in ``pg_stat_activity``. Set it to the task
            id so that "which task is holding this lock?" is answerable from a
            single query against the database, without correlating timestamps
            in the Airflow UI.
        statement_timeout_ms: Per-session statement timeout.
        autocommit: Left on by default so that transactions are opened
            explicitly by :func:`transaction` and are therefore visible.
        row_factory: ``dict_row`` by default - positional tuples make call
            sites break silently when a SELECT list grows.

    Raises:
        TransientError: the database could not be reached. Deliberately
            classified rather than propagated raw, so the Airflow retry policy
            can act on the category instead of on an isinstance chain.
    """
    try:
        conn = psycopg.connect(
            dsn,
            autocommit=autocommit,
            row_factory=row_factory,
            application_name=application_name,
        )
    except _TRANSIENT_PG_ERRORS as exc:
        raise TransientError(
            f"Could not connect to PostgreSQL: {exc}",
            application_name=application_name,
        ) from exc

    try:
        with conn.cursor() as cur:
            # set_config() rather than SET, because SET is DDL-ish and does not
            # accept bound parameters - and building the statement by string
            # formatting instead would be the one place in this codebase where
            # a value is interpolated into SQL.
            cur.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (str(statement_timeout_ms),),
            )
            # Every timestamp this platform reads or writes is UTC. Pinning the
            # session timezone means a developer's TZ environment variable can
            # never change what a query returns - business dates are derived
            # explicitly with AT TIME ZONE 'Asia/Kolkata', never implicitly.
            cur.execute("SELECT set_config('timezone', 'UTC', false)")
        yield conn
    finally:
        conn.close()


@contextmanager
def warehouse_connection(
    *, application_name: str = "volthive", **kwargs: Any
) -> Iterator[psycopg.Connection]:
    """Connect to the warehouse as the least-privilege ETL role."""
    settings = get_settings()
    with connect(settings.warehouse_dsn, application_name=application_name, **kwargs) as conn:
        yield conn


@contextmanager
def cms_connection(
    *, application_name: str = "volthive", **kwargs: Any
) -> Iterator[psycopg.Connection]:
    """Connect to the simulated source OLTP as the READ-ONLY role.

    Read-only is not a convention here, it is a grant: ``cms_reader`` has
    SELECT and nothing else. "The pipeline cannot corrupt its own source" is
    therefore a fact about the database rather than a promise about the code.
    """
    settings = get_settings()
    with connect(settings.cms_dsn, application_name=application_name, **kwargs) as conn:
        yield conn


@contextmanager
def transaction(conn: psycopg.Connection) -> Iterator[psycopg.Connection]:
    """Run a block inside one transaction, committing only on clean exit.

    This is the boundary that makes crash recovery safe. The canonical use is:

    .. code-block:: python

        with transaction(conn):
            copy_rows(conn, "raw.ocpp_cdr", ...)
            write_load_stat(conn, ...)
            advance_watermark(conn, ...)     # <- last, inside the same tx

    If anything raises, the rows AND the watermark roll back together, so the
    next run re-extracts exactly the same window. There is no state in which
    the data landed but the pipeline forgot that it did.
    """
    with conn.transaction():
        yield conn


def fetch_all(
    conn: psycopg.Connection, sql: str, params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Execute a query and return every row as a dict."""
    from volthive.db.params import translate_named_params

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(translate_named_params(sql), params or {})
        return list(cur.fetchall())


def fetch_one(
    conn: psycopg.Connection, sql: str, params: dict[str, Any] | None = None
) -> dict[str, Any] | None:
    """Execute a query and return the first row, or None."""
    from volthive.db.params import translate_named_params

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(translate_named_params(sql), params or {})
        return cur.fetchone()


def fetch_value(conn: psycopg.Connection, sql: str, params: dict[str, Any] | None = None) -> Any:
    """Execute a query and return the first column of the first row.

    Returns None when the query produced no rows, which is why callers that
    need a number write ``fetch_value(...) or 0`` rather than assuming.
    """
    from volthive.db.params import translate_named_params

    with conn.cursor() as cur:
        cur.row_factory = psycopg.rows.tuple_row
        cur.execute(translate_named_params(sql), params or {})
        row = cur.fetchone()
        return None if row is None else row[0]
