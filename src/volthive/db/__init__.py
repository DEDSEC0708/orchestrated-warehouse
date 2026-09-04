"""Database access: connections, transactions, bulk loading and SQL files.

This package is deliberately thin. It knows how to talk to PostgreSQL and
nothing about EV charging: no business rules, no column names, no
transformation logic. Everything domain-specific lives either in ``sql/`` or in
the ingest/transform packages that call into this one.

The split matters for testability. These primitives are exercised against a
real PostgreSQL instance in the integration suite, and the layers above can be
reasoned about as "which SQL file, with which parameters, in which transaction"
rather than as a tangle of connection handling and business logic.
"""

from __future__ import annotations

from volthive.db.bulk import DEFAULT_CHUNK_SIZE, copy_rows
from volthive.db.connection import (
    DEFAULT_STATEMENT_TIMEOUT_MS,
    cms_connection,
    connect,
    fetch_all,
    fetch_one,
    fetch_value,
    transaction,
    warehouse_connection,
)
from volthive.db.params import extract_param_names, translate_named_params
from volthive.db.sqlfiles import (
    load_sql,
    run_sql,
    run_sql_dir,
    run_sql_file,
    split_statements,
    sql_dir,
)

__all__ = [
    "DEFAULT_CHUNK_SIZE",
    "DEFAULT_STATEMENT_TIMEOUT_MS",
    "cms_connection",
    "connect",
    "copy_rows",
    "extract_param_names",
    "fetch_all",
    "fetch_one",
    "fetch_value",
    "load_sql",
    "run_sql",
    "run_sql_dir",
    "run_sql_file",
    "split_statements",
    "sql_dir",
    "transaction",
    "translate_named_params",
    "warehouse_connection",
]
