"""Load and execute the ``.sql`` files that hold this project's set logic.

Every transformation - staging conformance, the SCD2 merges, the fact loads,
the mart rebuilds - is a file under ``sql/``, executed from here with bound
parameters. Nothing in ``src/volthive/transform/`` contains business logic; it
supplies parameters and owns the transaction.

That split is deliberate. SQL in files is lintable by sqlfluff, reviewable as a
diff, runnable directly in ``psql`` when a load misbehaves, and visible to
anyone scrolling the repository on GitHub. SQL hidden in Python string
constants is none of those things.

Statements are split on semicolons at the top level so one file can contain a
DELETE followed by an INSERT and still be executed as a unit - which is exactly
the shape of every restatement-window load in this project.
"""

from __future__ import annotations

import time
from functools import lru_cache
from pathlib import Path
from typing import Any

import psycopg

from volthive.config.settings import repo_root
from volthive.db.params import extract_param_names, translate_named_params
from volthive.exceptions import ConfigurationError
from volthive.logging_setup import get_logger

__all__ = ["sql_dir", "load_sql", "split_statements", "run_sql", "run_sql_file", "run_sql_dir"]

log = get_logger(__name__)


def sql_dir() -> Path:
    """Directory holding the SQL files.

    Inside a container the repository is mounted at ``/opt/airflow`` and the
    package lives in ``/opt/airflow/src``, so deriving the path from the
    package location finds ``/opt/airflow/sql`` correctly there and
    ``<repo>/sql`` on a developer machine - without either needing to know
    which it is, and without depending on the working directory, which differs
    between Airflow, pytest and an interactive shell.
    """
    return repo_root() / "sql"


@lru_cache(maxsize=256)
def load_sql(relative_path: str) -> str:
    """Read a SQL file from ``sql/``, cached.

    Args:
        relative_path: Path below ``sql/``, e.g. ``"core/load_fact_session.sql"``.

    Raises:
        ConfigurationError: the file does not exist. A missing transformation
            file is a broken deployment, not a data problem - so it fails
            loudly rather than defaulting to a no-op, which would silently
            produce an empty table and report success.
    """
    path = sql_dir() / relative_path
    if not path.is_file():
        raise ConfigurationError(
            f"SQL file not found: {path}",
            entity=relative_path,
            expected=str(path),
        )
    return path.read_text(encoding="utf-8")


def split_statements(sql: str) -> list[str]:
    """Split a script into individual statements on top-level semicolons.

    Semicolons inside string literals, comments and dollar-quoted plpgsql
    bodies are not separators. Rather than re-implement that lexing, this
    reuses the scanner in :mod:`volthive.db.params`, which already tracks
    exactly those contexts: it walks the string once and records where each
    lexical region begins and ends.
    """
    statements: list[str] = []
    current: list[str] = []
    i = 0
    n = len(sql)

    while i < n:
        char = sql[i]

        if char == "-" and sql.startswith("--", i):
            end = sql.find("\n", i)
            end = n if end == -1 else end
            current.append(sql[i:end])
            i = end
            continue

        if char == "/" and sql.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if sql.startswith("/*", j):
                    depth += 1
                    j += 2
                elif sql.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            current.append(sql[i:j])
            i = j
            continue

        if char in {"'", '"'}:
            quote = char
            j = i + 1
            while j < n:
                if sql[j] == quote:
                    if j + 1 < n and sql[j + 1] == quote:
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            current.append(sql[i:j])
            i = j
            continue

        if char == "$":
            import re

            tag_match = re.match(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$", sql[i:])
            if tag_match:
                tag = tag_match.group(0)
                end = sql.find(tag, i + len(tag))
                end = n if end == -1 else end + len(tag)
                current.append(sql[i:end])
                i = end
                continue

        if char == ";":
            statements.append("".join(current))
            current = []
            i += 1
            continue

        current.append(char)
        i += 1

    statements.append("".join(current))
    return [s.strip() for s in statements if s.strip()]


def run_sql(
    conn: psycopg.Connection,
    sql_text: str,
    params: dict[str, Any] | None = None,
    *,
    origin: str = "<inline>",
) -> int:
    """Execute a (possibly multi-statement) script and return rows affected.

    Args:
        conn: An open connection. The CALLER owns the transaction - this
            function never commits, because the commit point of a restatement
            load belongs with the watermark update, not with the SQL runner.
        sql_text: The script.
        params: Values for the ``:name`` placeholders.
        origin: A label used in errors and logs, normally the file path.

    Returns:
        Rows affected by the LAST statement in the script. Every load file in
        this project ends with the statement whose count is the interesting
        one (the INSERT, not the preceding DELETE), so this is the number that
        goes into ``audit.load_stat``.

    Raises:
        ConfigurationError: the script references a placeholder that was not
            supplied. Caught here so the message names the file and the
            missing parameter, instead of surfacing as a bare KeyError from
            inside psycopg's binder.
    """
    supplied = set((params or {}).keys())
    required = extract_param_names(sql_text)
    missing = required - supplied
    if missing:
        raise ConfigurationError(
            f"Missing SQL parameters {sorted(missing)} for {origin}",
            entity=origin,
            expected=sorted(required),
            actual=sorted(supplied),
        )

    affected = 0
    started = time.monotonic()
    with conn.cursor() as cur:
        for statement in split_statements(sql_text):
            statement_params = extract_param_names(statement)
            if statement_params:
                cur.execute(
                    translate_named_params(statement),
                    {name: (params or {})[name] for name in statement_params},
                )
            else:
                # NO parameter argument at all, not an empty dict. psycopg only
                # interprets '%' as a placeholder when parameters are supplied,
                # so passing {} would make it choke on a legitimate '%I' inside
                # a plpgsql format() call in a DO block. Statements with no
                # placeholders are sent verbatim.
                cur.execute(statement)
            if cur.rowcount is not None and cur.rowcount >= 0:
                affected = cur.rowcount

    log.debug(
        "sql_executed",
        origin=origin,
        rows_affected=affected,
        duration_ms=int((time.monotonic() - started) * 1000),
    )
    return affected


def run_sql_file(
    conn: psycopg.Connection,
    relative_path: str,
    params: dict[str, Any] | None = None,
) -> int:
    """Execute a file from ``sql/`` and return rows affected by its last statement."""
    return run_sql(conn, load_sql(relative_path), params, origin=relative_path)


def run_sql_dir(
    conn: psycopg.Connection,
    relative_dir: str,
    params: dict[str, Any] | None = None,
) -> list[tuple[str, int]]:
    """Execute every ``.sql`` file in a directory, in FILENAME ORDER.

    Filename order is why the DDL files are numbered ``00_``, ``01_``, ``10_``.
    Dependencies between them are real - the fact tables' foreign keys need the
    dimensions to exist - and encoding the order in the filenames keeps it
    visible in a directory listing rather than hidden in a Python list that can
    drift out of step with the files.

    Returns:
        ``(relative_path, rows_affected)`` for each file executed.
    """
    directory = sql_dir() / relative_dir
    if not directory.is_dir():
        raise ConfigurationError(
            f"SQL directory not found: {directory}",
            entity=relative_dir,
            expected=str(directory),
        )

    results: list[tuple[str, int]] = []
    for path in sorted(directory.glob("*.sql")):
        rel = f"{relative_dir}/{path.name}"
        results.append((rel, run_sql_file(conn, rel, params)))
    return results
